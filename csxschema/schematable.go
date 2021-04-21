package csxschema

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	amqp "gitlab.com/battler/modules/amqpconnector"
	"gitlab.com/battler/modules/csxstrings"
	"gitlab.com/battler/modules/csxutils"
	"gitlab.com/battler/modules/reporter"
	dbc "gitlab.com/battler/modules/sql"
)

var (
	registerSchema          = registerSchemeManager{}
	disableRegisterMetadata = os.Getenv("DISABLE_REGISTER_METADATA") == "1"
	amqpURI                 = os.Getenv("AMQP_URI")
	sqlURIS                 = os.Getenv("SQL_URIS")
	moduleInited            = false
)

//---------------------------------------------------------------------------
// Database schemas defenitions

// SchemaTableMetadata Structure for storing metadata
type SchemaTableMetadata struct {
	ID         string    `db:"id" json:"id"`
	Collection string    `db:"collection" json:"collection"`
	MetaData   dbc.JsonB `db:"metadata" json:"metadata"`
}

// SchemaField is definition of sql field
type SchemaField struct {
	Name      string
	Type      string
	IndexType string
	Length    int
	IsNull    bool
	IsUUID    bool
	Default   string
	Key       int
	Auto      int
	checked   bool
	Sequence  string
}

// SchemaTable is definition of sql table
type SchemaTable struct {
	DB           *sqlx.DB
	DatabaseName string
	Name         string
	Fields       []*SchemaField
	initalized   bool
	IDFieldName  string
	sqlSelect    string
	SQLFields    []string
	// GenFields   []string
	getAmqpUpdateData map[string]SchemaTableAmqpDataCallback
	ExtKeys           []string
	Extensions        []string
	onUpdate          schemaTableUpdateCallback
	LogChanges        bool
	ExportFields      string
	ExportTables      string
	Struct            interface{}
}

// SchemaTableAmqpDataCallback is using for get data for amqp update
type SchemaTableAmqpDataCallback func(id string) interface{}
type schemaTablePrepareCallback func(table *SchemaTable, event string)
type schemaTableUpdateCallback func(table *SchemaTable, msg interface{})
type schemaTableReg struct {
	table       *SchemaTable
	callbacks   []schemaTableUpdateCallback
	isConnector bool
}
type schemaTableMap map[string]*schemaTableReg

type registerSchemeManager struct {
	sync.RWMutex
	tables    schemaTableMap
	databases map[string]*sqlx.DB
}

func applyAmqpUpdates(table, id string, queryObj *dbc.Query) {
	registerSchema.RLock()
	schemaTableReg, ok := registerSchema.tables[table]
	registerSchema.RUnlock()
	if ok {
		if schemaTableReg.table != nil && schemaTableReg.table.getAmqpUpdateData != nil {
			for routingKey, dataCallback := range schemaTableReg.table.getAmqpUpdateData {
				updateCallback := func() {
					data := dataCallback(id)
					go amqp.SendUpdate(amqpURI, routingKey, id, "update", data)
				}
				if queryObj.IsTransact() {
					queryObj.BindTxCommitCallback(updateCallback)
				} else {
					updateCallback()
				}
			}
		}
	}
}
func schemaLogSQL(sql string, err error) {
	if err != nil {
		logrus.Error(sql, ": ", err)
	} else {
		logrus.Info(sql)
	}
}

func (reg *registerSchemeManager) connect(databases map[string]*dbc.DatabaseParams) {
	reg.Lock()
	defer reg.Unlock()
	reg.databases = map[string]*sqlx.DB{}
	for databaseName, databaseParams := range databases {
		reg.initDatabase(databaseName, databaseParams)
	}
	logrus.Info(`connect to db in prepare scheme manager ended`)
}

func (reg *registerSchemeManager) prepare() {
	registerSchema.Lock()
	defer registerSchema.Unlock()
	if registerSchema.tables == nil {
		return
	}
	for _, tableManager := range registerSchema.tables {
		err := tableManager.table.prepare()
		if err != nil {
			logrus.Error(`Table "` + tableManager.table.Name + `" schema check failed: ` + err.Error())
		} else {
			logrus.Debug(`Table "` + tableManager.table.Name + `" schema check successfully`)
		}
	}
	logrus.Info(`prepare scheme manager success ended`)
}

func registerSchemaSetUpdateCallback(tableName string, cb schemaTableUpdateCallback, internal bool) error {
	if !internal {
		registerSchema.Lock()
		defer registerSchema.Unlock()
	}
	if registerSchema.tables == nil {
		return errors.New("Table not registered")
	}
	reg, ok := registerSchema.tables[tableName]
	if !ok {
		return errors.New("Table not registered")
	}
	reg.callbacks = append(reg.callbacks, cb)
	if !reg.isConnector {
		reg.isConnector = true
		queueName := "csx.sql." + tableName
		envName := os.Getenv("CSX_ENV")
		consumerTag := os.Getenv("SERVICE_NAME")
		if envName != "" {
			queueName += "." + envName
		}
		if consumerTag != "" {
			queueName += "." + consumerTag
		} else {
			queueName += "." + *csxstrings.NewId()
		}
		go amqp.OnUpdates(registerSchemaOnUpdate, []string{tableName})
	}
	return nil
}

func registerSchemaOnUpdate(d *amqp.Delivery) {
	msg := amqp.Update{}
	err := json.Unmarshal(d.Body, &msg)
	if err != nil {
		logrus.Error("HandleUpdates", "Error parse json: ", err)
		return
	}
	registerSchema.RLock()
	name := d.RoutingKey
	reg, ok := registerSchema.tables[name]
	if ok {
		for _, cb := range reg.callbacks {
			cb(reg.table, msg)
		}
	}
	registerSchema.RUnlock()
}

// GetSchemaTable get schema table by table name
func GetSchemaTable(table string) (*SchemaTable, bool) {
	registerSchema.RLock()
	schema, ok := registerSchema.tables[table]
	registerSchema.RUnlock()
	if !ok {
		return nil, ok
	}
	return schema.table, ok
}

// GetSchemaTablesIds return tables names array
func GetSchemaTablesIds() (res []string) {
	res = make([]string, 0)
	registerSchema.Lock()
	for _, schemaReg := range registerSchema.tables {
		res = append(res, schemaReg.table.Name)
	}
	registerSchema.Unlock()
	return res
}

// GetSchemaTablesExtKeys return ext routing keys for amqp binding
func GetSchemaTablesExtKeys() (res []string) {
	res = make([]string, 0)
	registerSchema.Lock()
	for _, schemaReg := range registerSchema.tables {
		res = append(res, schemaReg.table.ExtKeys...)
	}
	registerSchema.Unlock()
	return res
}

// NewSchemaField create SchemaTable definition
func NewSchemaField(name, typ string, args ...interface{}) *SchemaField {
	field := new(SchemaField)
	field.Name = name
	field.Type = typ
	ofs := 2
	obj := reflect.ValueOf(field).Elem()
	for index, arg := range args {
		f := obj.Field(index + ofs)
		v := reflect.ValueOf(arg)
		f.Set(v)
	}
	return field
}

// NewSchemaTableFields create SchemaTable definition
func NewSchemaTableFields(name string, fieldsInfo ...*SchemaField) *SchemaTable {
	fields := make([]*SchemaField, len(fieldsInfo))
	for index, field := range fieldsInfo {
		fields[index] = field
	}
	newSchemaTable := SchemaTable{}
	newSchemaTable.Name = name
	newSchemaTable.Fields = fields
	newSchemaTable.register()
	return &newSchemaTable
}

// NewSchemaTable create SchemaTable definition from record structure
func NewSchemaTable(name string, info interface{}, options map[string]interface{}) *SchemaTable {
	var recType reflect.Type
	infoType := reflect.TypeOf(info)
	switch infoType.Kind() {
	case reflect.Slice:
		recType = infoType.Elem()
	case reflect.Array:
		recType = infoType.Elem()
	default:
		recType = infoType
	}

	newSchemaTable := SchemaTable{
		Extensions: []string{},
	}
	// genFields := []string{}
	idFieldName := ""
	fields := make([]*SchemaField, 0)
	if recType.Kind() == reflect.Struct {
		fcnt := recType.NumField()
		for i := 0; i < fcnt; i++ {
			f := recType.Field(i)
			name := f.Tag.Get("db")
			dbFieldTag := f.Tag.Get("dbField")
			if len(name) == 0 || name == "-" || dbFieldTag == "-" {
				continue
			}
			field := new(SchemaField)
			field.Name = name
			field.Type = f.Tag.Get("type")
			field.IndexType = f.Tag.Get("index")
			if len(field.Type) == 0 {
				field.Type = f.Type.Name()
				if len(field.Type) == 0 || field.Type == "string" {
					field.Type = "varchar"
				}
			}
			field.Length, _ = strconv.Atoi(f.Tag.Get("len"))
			field.Key, _ = strconv.Atoi(f.Tag.Get("key"))
			field.Default = f.Tag.Get("def")
			field.Auto, _ = strconv.Atoi(f.Tag.Get("auto"))
			if f.Type.Kind() == reflect.Ptr || f.Type.Kind() == reflect.Map {
				field.IsNull = true
			}
			field.IsUUID = field.Type == "uuid"
			if (field.Key == 1 || name == "id") && field.IsUUID {
				idFieldName = name
			}
			field.Sequence = f.Tag.Get("sequence")
			fields = append(fields, field)
			extension := f.Tag.Get("ext")
			if len(extension) > 0 {
				newSchemaTable.Extensions = append(newSchemaTable.Extensions, extension)
			}
			// if field.Default != "" || field.Auto != 0 {
			// 	genFields = append(genFields, name)
			// }
		}
	}

	var onUpdate schemaTableUpdateCallback
	var getAmqpUpdateData map[string]SchemaTableAmqpDataCallback
	var logChanges bool
	var extKeys []string
	for key, val := range options {
		switch key {
		case "onUpdate":
			if reflect.TypeOf(val).Kind() == reflect.Func {
				onUpdate = val.(func(table *SchemaTable, msg interface{}))
			}
		case "logChanges":
			logChanges = val.(bool)
		case "getAmqpUpdateData":
			getAmqpUpdateData = val.(map[string]SchemaTableAmqpDataCallback)
		case "extKeys":
			extKeys = val.([]string)
		case "db":
			newSchemaTable.DB = val.(*sqlx.DB)
		case "databaseName":
			newSchemaTable.DatabaseName = val.(string)
		}
	}
	newSchemaTable.Name = name
	newSchemaTable.Fields = fields
	newSchemaTable.IDFieldName = idFieldName
	newSchemaTable.onUpdate = onUpdate
	newSchemaTable.getAmqpUpdateData = getAmqpUpdateData
	newSchemaTable.ExtKeys = extKeys
	newSchemaTable.LogChanges = logChanges
	newSchemaTable.Struct = info
	// newSchemaTable.GenFields = genFields
	newSchemaTable.register()
	return &newSchemaTable
}

func (table *SchemaTable) register() {
	registerSchema.Lock()
	if registerSchema.tables == nil {
		registerSchema.tables = make(schemaTableMap)
	}
	reg, ok := registerSchema.tables[table.Name]
	if !ok {
		reg = &schemaTableReg{table, []schemaTableUpdateCallback{}, false}
		registerSchema.tables[table.Name] = reg
	}
	registerSchema.Unlock()
}

func (table *SchemaTable) registerMetadata() {
	res := SchemaTableMetadata{}
	err := table.DB.Get(&res, "SELECT id, collection, metadata FROM metadata WHERE collection = '"+table.Name+"'")
	if err != nil {
		if err == sql.ErrNoRows {
			return
		}
		logrus.Error("error get schema table metadata for collection: ", table.Name, " error: ", err)
		return
	}
	metadataInt, ok := res.MetaData["export"]
	if !ok {
		return
	}
	metadata, ok := metadataInt.(map[string]interface{})
	if !ok {
		return
	}
	table.registerExportMetadata(metadata)
}

// RestrictRolesRights return complex restrict query based on all roles
func (table *SchemaTable) RestrictRolesRights(roles map[string]*dbc.JsonB) string {
	restrictQuery := ""
	for _, rights := range roles {
		if restrictQuery != "" {
			restrictQuery += " OR "
		}
		restrictVal := table.GetRestrictQuery(rights)
		if restrictVal != "" {
			restrictQuery += "( " + restrictVal + " )"
		}
	}
	return restrictQuery
}

// GetRestrictQuery returns sql restrict query by roles rights
func (table *SchemaTable) GetRestrictQuery(rights *dbc.JsonB) string {
	if rights == nil {
		return ""
	}
	restrictFieldsInt, ok := (*rights)[table.Name]
	if !ok {
		// if entity collection not found in rights map is FULL ACCESS
		return ""
	}
	restrictFields, ok := restrictFieldsInt.(map[string]interface{})
	if !ok {
		// ignore invalid rights
		return ""
	}
	restrictQuery := ""
	for field, value := range restrictFields {
		restrictField := reflect.ValueOf(value)
		if restrictField.Kind() == reflect.Ptr {
			restrictField = reflect.Indirect(restrictField)
		}
		var restrictVal string

		switch restrictField.Interface().(type) {
		case int, int8, int16, int32, int64:
			restrictVal = strconv.FormatInt(restrictField.Int(), 10)
		case uint, uint8, uint16, uint32, uint64:
			restrictVal = strconv.FormatUint(restrictField.Uint(), 10)
		case float32, float64:
			restrictVal = strconv.FormatFloat(restrictField.Float(), 'E', -1, 64)
		case string:
			restrictVal = restrictField.String()
		case bool:
			restrictVal = strconv.FormatBool(restrictField.Bool())
		default:
			arrayValues, ok := value.([]interface{})
			if !ok {
				continue
			}
			inValues := ""
			for i := 0; i < len(arrayValues); i++ {
				if i > 0 && inValues != "" {
					inValues += ","
				}
				if val, ok := arrayValues[i].(string); ok {
					inValues += "'" + val + "'"
				}
			}
			if inValues != "" {
				if restrictQuery != "" {
					restrictQuery += " AND "
				}
				restrictQuery += `"` + table.Name + `".` + field + " IN (" + inValues + ")"
			}
			continue
		}

		if restrictVal != "" {
			if restrictQuery != "" {
				restrictQuery += " AND "
			}
			restrictQuery += `"` + table.Name + `".` + field + " = '" + restrictVal + "'"
		}
	}
	return restrictQuery
}

func (table *SchemaTable) registerExportMetadata(metadata map[string]interface{}) {
	exportFieldsInt, ok := metadata["fields"]
	if !ok {
		return
	}
	exportFields, ok := exportFieldsInt.(string)
	if !ok {
		return
	}
	table.ExportFields = exportFields
	exportTablesInt, ok := metadata["tables"]
	if !ok {
		return
	}
	exportTables, ok := exportTablesInt.(string)
	if !ok {
		return
	}
	table.ExportTables = exportTables
}

// Export is using for get xlsx bytes
func (table *SchemaTable) Export(params map[string]string, extConditions ...string) []byte {
	params["limit"] = "100000"
	params["fields"] = table.ExportFields
	params["join"] = table.ExportTables
	var query string
	if len(extConditions) > 0 {
		query = dbc.MakeQueryFromReq(params, extConditions[0])
	} else {
		query = dbc.MakeQueryFromReq(params)
	}
	xlsx := bytes.NewBuffer([]byte{})
	_ = dbc.ExecQuery(&query, func(rows *sqlx.Rows) bool {
		reporter.GenerateXLSXFromRows(rows, xlsx)
		return true
	})
	return xlsx.Bytes()
}

func (field *SchemaField) sql() string {
	sql := `"` + field.Name + `" ` + field.Type
	if field.Length > 0 {
		sql += "(" + strconv.Itoa(field.Length) + ")"
	}
	if !field.IsNull {
		sql += " NOT NULL"
	}
	if len(field.Default) > 0 {
		sql += " DEFAULT " + field.Default
	}
	return sql
}

func (table *SchemaTable) create() error {
	tx, err := table.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Commit()
	var keys string
	skeys := []*SchemaField{}
	sql := `CREATE TABLE "` + table.Name + `"(`
	// enable extensions
	for i := 0; i < len(table.Extensions); i++ {
		ext := table.Extensions[i]
		sql := `CREATE EXTENSION IF NOT EXISTS "` + ext + `"`
		_, err := table.DB.Exec(sql)
		schemaLogSQL(sql, err)
	}
	sqlSequence := ""
	for index, field := range table.Fields {
		if index > 0 {
			sql += ", "
		}
		sql += field.sql()
		if (field.Key & 1) != 0 {
			if len(keys) > 0 {
				keys += ","
			}
			keys += field.Name
		}
		if (field.Key&2) != 0 || field.IndexType != "" {
			skeys = append(skeys, field)
		}
		if len(field.Sequence) > 0 {
			sqlSequence += `CREATE SEQUENCE "` + table.Name + `_` + field.Sequence + `"; `
		}
	}
	sql += `)`
	_, err = table.DB.Exec(sql)
	schemaLogSQL(sql, err)
	if err == nil && len(keys) > 0 {
		sql := `ALTER TABLE "` + table.Name + `" ADD PRIMARY KEY (` + keys + `)`
		_, err = table.DB.Exec(sql)
		schemaLogSQL(sql, err)
	}
	if err == nil && len(skeys) > 0 {
		for _, field := range skeys {
			table.createIndex(field)
		}
	}
	// insert mandat info
	sqlBaseMandat := `INSERT INTO "mandatInfo" ("id", "group", "subject")
	SELECT uuid_generate_v4(), 'base', '` + table.Name + `'
	WHERE NOT EXISTS (SELECT id FROM "mandatInfo" WHERE "subject" = '` + table.Name + `' and "group" = 'base')
	RETURNING id;`
	_, err = table.DB.Exec(sqlBaseMandat)
	schemaLogSQL(sqlBaseMandat, err)
	if len(sqlSequence) > 0 {
		_, err = dbc.DB.Exec(sqlSequence)
		schemaLogSQL(sqlSequence, err)
	}
	return err
}

func (table *SchemaTable) createIndex(field *SchemaField) error {
	indexType := field.IndexType
	if indexType == "" {
		if field.Type == "json" || field.Type == "jsonb" {
			indexType = "GIN"
		} else {
			indexType = "btree"
		}
	}
	sql := `CREATE INDEX "` + table.Name + "_" + field.Name + `_idx" ON "` + table.Name + `" USING ` + indexType + ` ("` + field.Name + `")`
	_, err := table.DB.Exec(sql)
	schemaLogSQL(sql, err)
	return err
}

func (table *SchemaTable) alter(cols []string) error {
	for _, name := range cols {
		_, field := table.FindField(name)
		if field != nil {
			field.checked = true
		}
	}
	var err error
	for _, field := range table.Fields {
		if !field.checked {
			sql := `ALTER TABLE "` + table.Name + `" ADD COLUMN ` + field.sql()
			_, err = table.DB.Exec(sql)
			schemaLogSQL(sql, err)
			if err != nil {
				break
			}
			if (field.Key&2) != 0 || field.IndexType != "" {
				table.createIndex(field)
			}
			field.checked = true
		}
	}
	return err
}

// ParseConnectionString parse connection string map with params
func ParseConnectionString(connString string) (params map[string]string) {
	strArray := strings.Split(connString, " ")
	params = map[string]string{}
	for _, v := range strArray {
		keyValArray := strings.Split(v, "=")
		if len(keyValArray) != 2 {
			continue
		}
		params[keyValArray[0]] = keyValArray[1]
	}
	return params
}

// GetDatabase get database pointer from schema manager map by name
func (reg *registerSchemeManager) GetDatabase(databaseName string) (*sqlx.DB, bool) {
	db, ok := reg.databases[databaseName]
	return db, ok
}

// SetDatabase set database pointer in schema manager map by name
func (reg *registerSchemeManager) SetDatabase(databaseName string, db *sqlx.DB) {
	reg.databases[databaseName] = db
}

// Prepare check scheme initializing and and create or alter table
func (reg *registerSchemeManager) initDatabase(databaseName string, databaseParams *dbc.DatabaseParams) {
	if databaseParams == nil {
		logrus.Error("nil database params, databaseName: ", databaseName)
		return
	}
	if databaseParams.ConnectionString == "" {
		logrus.Error("empty database connectionString, databaseName: ", databaseName)
		return
	}
	if databaseParams.DriverName == "" {
		logrus.Warn("connect to database: ", databaseName, " without set type, use postgres driver by default")
		// default database type
		// need for compatible with old code
		databaseParams.DriverName = "postgres"
	}
	connectionString := databaseParams.ConnectionString
	db, err := sqlx.Connect(databaseParams.DriverName, databaseParams.ConnectionString)
	if err != nil {
		logrus.Error("failed connect to database:", connectionString, " ", err)
		time.Sleep(20 * time.Second)
		reg.initDatabase(databaseName, databaseParams)
	} else {
		reg.SetDatabase(databaseName, db)
	}
}

// Prepare check scheme initializing and and create or alter table
func (table *SchemaTable) prepare() error {
	if table.DatabaseName != "" {
		db, ok := registerSchema.GetDatabase(table.DatabaseName)
		if !ok {
			return errors.New("wrong database name: " + table.DatabaseName + " in schema: " + table.Name)
		}
		table.DB = db
	} else {
		table.DB = dbc.DB
	}
	table.initalized = true
	table.SQLFields = []string{}
	table.sqlSelect = "SELECT "
	for index, field := range table.Fields {
		if index > 0 {
			table.sqlSelect += ", "
		}
		fieldName := ""
		if field.Type == "geometry" {
			fieldName = "st_asgeojson(" + field.Name + `) as "` + field.Name + `"`
		} else {
			fieldName = `"` + field.Name + `" `
		}
		table.sqlSelect += fieldName
		table.SQLFields = append(table.SQLFields, fieldName)
	}
	table.sqlSelect += ` FROM "` + table.Name + `"`

	rows, err := table.DB.Query(`SELECT * FROM "` + table.Name + `" limit 1`)
	if err == nil {
		defer rows.Close()
		var cols []string
		cols, err = rows.Columns()
		if err == nil {
			err = table.alter(cols)
		}
	} else {
		code := dbc.GetDBErrorCode(err)
		if code == dbc.TABLE_NOT_EXISTS { // not exists
			err = table.create()
		}
	}
	if err == nil && table.onUpdate != nil {
		registerSchemaSetUpdateCallback(table.Name, table.onUpdate, true)
	}
	if !disableRegisterMetadata {
		table.registerMetadata()
	}
	return err
}

// FindField search field by name
func (table *SchemaTable) FindField(name string) (int, *SchemaField) {
	for index, field := range table.Fields {
		if field.Name == name {
			return index, field
		}
	}
	return -1, nil
}

// OnUpdate init and set callback on table external update event
func (table *SchemaTable) OnUpdate(cb schemaTableUpdateCallback) {
	registerSchemaSetUpdateCallback(table.Name, cb, false)
}

// QueryParams execute  sql query with params
func (table *SchemaTable) QueryParams(recs interface{}, params ...[]string) error {
	var fields, where, order, groupby *[]string

	if len(params) > 0 {
		where = &params[0]
	}
	if len(params) > 1 {
		order = &params[1]
	}
	if len(params) > 2 {
		fields = &params[2]
	} else {
		fields = &table.SQLFields
	}
	if len(params) > 3 && params[3] != nil {
		join := &params[3]
		return table.QueryJoin(recs, fields, where, order, join)
	}
	if len(params) > 4 {
		groupby = &params[4]
	}

	return table.Query(recs, fields, where, order, groupby)
}

// Query execute sql query with params
func (table *SchemaTable) Query(recs interface{}, fields, where, order, group *[]string, args ...interface{}) error {
	qparams := &dbc.QueryParams{
		Select: fields,
		From:   &table.Name,
		Where:  where,
		Order:  order,
		Group:  group,
	}

	query, err := dbc.MakeQuery(qparams)
	if err = table.DB.Select(recs, *query, args...); err != nil && err != sql.ErrNoRows {
		logrus.Error("err: ", err, " query:", *query)
		return err
	}
	return nil
}

// QueryJoin execute sql query with params
func (table *SchemaTable) QueryJoin(recs interface{}, fields, where, order, join *[]string, args ...interface{}) error {
	if join == nil || len(*join) == 0 {
		return errors.New("join arg is empty")
	}
	qparams := &dbc.QueryParams{
		Select: fields,
		From:   &(*join)[0],
		Where:  where,
		Order:  order,
	}

	query, err := dbc.MakeQuery(qparams)
	if err = table.DB.Select(recs, *query, args...); err != nil && err != sql.ErrNoRows {
		logrus.Error(*query)
		fmt.Println(err)
		return err
	}
	return nil
}

// Select execute select sql string
func (table *SchemaTable) Select(recs interface{}, where string, args ...interface{}) error {
	sql := table.sqlSelect
	if len(where) > 0 {
		sql += " WHERE " + where
	}
	return table.DB.Select(recs, sql, args...)
}

// Get execute select sql string and return first record
func (table *SchemaTable) Get(rec interface{}, where string, args ...interface{}) error {
	sql := table.sqlSelect
	if len(where) > 0 {
		sql += " WHERE " + where
	}
	return table.DB.Get(rec, sql, args...)
}

// Count records with where sql string
func (table *SchemaTable) Count(where string, args ...interface{}) (int, error) {
	sql := `SELECT COUNT(*) FROM "` + table.Name + `"`
	if len(where) > 0 {
		sql += " WHERE " + where
	}
	rec := struct{ Count int }{}
	err := table.DB.Get(&rec, sql, args...)
	if err == nil {
		return rec.Count, err
	}
	return -1, err
}

// Exists test records exists with where sql string
func (table *SchemaTable) Exists(where string, args ...interface{}) (bool, error) {
	cnt, err := table.Count(where, args...)
	return cnt > 0, err
}

// TransactInsert execute insert sql string in transaction
func (table *SchemaTable) TransactInsert(data interface{}, query *dbc.Query, options ...map[string]interface{}) error {
	return table.insert(data, query, options...)
}

// Insert execute insert sql string
func (table *SchemaTable) Insert(data interface{}, options ...map[string]interface{}) error {
	return table.insert(data, nil, options...)
}

// insert execute insert sql string
func (table *SchemaTable) insert(data interface{}, query *dbc.Query, options ...map[string]interface{}) error {
	_, err := table.checkInsert(data, nil, query, options...)
	return err
}

func (table *SchemaTable) getIDField(id string, options []map[string]interface{}) (idField, newID string, err error) {
	idField = table.IDFieldName
	newID = id
	if len(options) > 0 {
		option := options[0]
		if option != nil && option["idField"] != nil {
			idField = option["idField"].(string)
		}

		index, field := table.FindField(idField)
		if index == -1 {
			return "", "", errors.New("invalid id field: " + idField)
		}
		if field.Type == "uuid" && id == "" {
			newID = *csxstrings.NewId()
		}
	}
	if idField == "" {
		idField = "id"
	}
	return idField, newID, nil
}

// TransactUpsertMultiple execute insert or update sql string in transaction
func (table *SchemaTable) TransactUpsertMultiple(query *dbc.Query, data interface{}, where string, options ...map[string]interface{}) (count int64, err error) {
	return table.upsertMultiple(query, data, where, options...)
}

// UpsertMultiple execute insert or update sql string
func (table *SchemaTable) UpsertMultiple(data interface{}, where string, options ...map[string]interface{}) (count int64, err error) {
	return table.upsertMultiple(nil, data, where, options...)
}

// UpsertMultiple execute insert or update sql string
func (table *SchemaTable) upsertMultiple(query *dbc.Query, data interface{}, where string, options ...map[string]interface{}) (count int64, err error) {
	result, err := table.checkInsert(data, &where, query, options...)
	if err != nil {
		return count, err
	}
	count, err = result.RowsAffected()
	if err != nil {
		_, _, _, err = table.updateMultiple(nil, data, where, query, options...)
	}
	return count, err
}

// Upsert execute insert or update sql string
func (table *SchemaTable) Upsert(id string, data interface{}, options ...map[string]interface{}) error {
	return table.upsert(id, data, nil, options...)
}

// TransactUpsert execute insert or update sql string in transaction
func (table *SchemaTable) TransactUpsert(id string, data interface{}, query *dbc.Query, options ...map[string]interface{}) error {
	return table.upsert(id, data, query, options...)
}

// Upsert execute insert or update sql string
func (table *SchemaTable) upsert(id string, data interface{}, query *dbc.Query, options ...map[string]interface{}) error {
	idField, id, err := table.getIDField(id, options)
	if err != nil {
		return err
	}
	where := `"` + idField + `"='` + id + "'"
	result, err := table.checkInsert(data, &where, query, options...)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err == nil && rows == 0 {
		err = table.update(id, data, query, options...)
	}
	return err
}

func (table *SchemaTable) prepareArgsStruct(rec reflect.Value, oldData interface{}, idField string, options ...map[string]interface{}) (args []interface{}, values, fields, itemID string, diff, diffPub map[string]interface{}) {
	diff = make(map[string]interface{})
	diffPub = make(map[string]interface{})
	args = []interface{}{}
	cnt := 0
	recType := rec.Type()
	fcnt := recType.NumField()
	oldRec := reflect.ValueOf(oldData)
	compareWithOldRec := oldRec.IsValid()
	for i := 0; i < fcnt; i++ {
		f := recType.Field(i)
		name := f.Tag.Get("db")
		tag := recType.Field(i).Tag.Get("db")
		tagWrite := recType.Field(i).Tag.Get("dbField")
		if tagWrite == "-" || tag == "-" ||
			(tag == "" && tagWrite == "") {
			continue
		}
		fType := f.Tag.Get("type")
		if len(name) == 0 || name == "-" {
			continue
		}
		newFld := rec.FieldByName(f.Name)
		var oldFldInt interface{}
		if compareWithOldRec {
			oldFld := oldRec.FieldByName(f.Name)
			if oldFld.IsValid() {
				oldFldInt = oldFld.Interface()
				if oldFldInt == newFld.Interface() {
					continue
				}
			}
		}
		if checkExcludeFields(name, options...) {
			continue
		}
		if cnt > 0 {
			fields += ","
			values += ","
		}
		cnt++
		fields += `"` + name + `"`
		fldInt := newFld.Interface()
		if newFld.IsValid() {
			if fType == "geometry" {
				values += "ST_GeomFromGeoJSON($" + strconv.Itoa(cnt) + ")"
			} else {
				values += "$" + strconv.Itoa(cnt)
			}
			if name == idField {
				itemID = fmt.Sprintf("%v", fldInt)
			}
			if name != idField || oldFldInt == nil {
				diffVal := []interface{}{fldInt}
				if oldFldInt != nil {
					diffVal = append(diffVal, oldFldInt)
				}
				diff[name] = diffVal
				diffPub[name] = fldInt
			}
			args = append(args, fldInt)
		} else {
			values += "NULL"
		}
	}
	excludeFields(diff, diffPub, options...)
	return args, values, fields, itemID, diff, diffPub
}

func (table *SchemaTable) prepareArgsMap(data, oldData map[string]interface{}, idField string, options ...map[string]interface{}) (args []interface{}, values, fields, itemID string, diff, diffPub map[string]interface{}) {
	diff = make(map[string]interface{})
	diffPub = make(map[string]interface{})
	args = []interface{}{}
	cnt := 0
	cntField := 0
	compareWithOldRec := oldData != nil
	for name := range data {
		_, f := table.FindField(name)
		if f == nil {
			logrus.WithFields(logrus.Fields{"table": table.Name, "field": name}).Warn("invalid field")
			continue
		}
		val := data[name]
		oldVal := oldData[name]
		if f.IsUUID {
			if str, ok := val.(string); ok && str == "" {
				val = nil
			}
		}
		if compareWithOldRec && val == oldVal {
			continue
		}
		if checkExcludeFields(name, options...) {
			continue
		}
		if cntField > 0 {
			fields += ","
			values += ","
		}
		fields += `"` + name + `"`
		if name == idField {
			itemID = fmt.Sprintf("%v", val)
		}
		if name != idField || oldVal == nil {
			diffVal := []interface{}{val}
			if oldVal != nil {
				diffVal = append(diffVal, oldVal)
			}
			diff[name] = diffVal
			diffPub[name] = val
		}
		if f.Type == "uuid[]" {
			val = pq.Array(val)
		}
		if val != nil {
			cnt++
			argNum := "$" + strconv.Itoa(cnt)
			if f.Type == "geometry" {
				values += "ST_GeomFromGeoJSON(" + argNum + ")"
			} else if f.Type == "jsonb" {
				valJSON, err := json.Marshal(val)
				if err != nil {
					logrus.Error("invalid jsonb value: ", val, " of field: ", name)
				}
				val = valJSON
				values += argNum
			} else {
				values += argNum
			}
			args = append(args, val)
		} else {
			values += "NULL"
		}
		cntField++
	}
	excludeFields(diff, diffPub, options...)
	return args, values, fields, itemID, diff, diffPub
}

func excludeFields(diff map[string]interface{}, diffPub map[string]interface{}, options ...map[string]interface{}) {
	ignoreDiff := []string{}
	if len(options) > 0 {
		option := options[0]
		if option["ignoreDiff"] != nil {
			ignoreDiff = option["ignoreDiff"].([]string)
		}
	}
	for i := 0; i < len(ignoreDiff); i++ {
		field := ignoreDiff[i]
		delete(diff, field)
		delete(diffPub, field)
	}
}

func checkExcludeFields(fieldName string, options ...map[string]interface{}) bool {
	ignoreDiff := []string{}
	if len(options) > 0 {
		option := options[0]
		if option["ignoreDiff"] != nil {
			ignoreDiff = option["ignoreDiff"].([]string)
		}
	}
	for i := 0; i < len(ignoreDiff); i++ {
		f := ignoreDiff[i]
		if f == fieldName {
			return true
		}
	}
	return false
}

// SelectMap select multiple items from db to []map[string]interfaces
func (table *SchemaTable) SelectMap(where string) ([]map[string]interface{}, error) {
	q := table.sqlSelect
	if len(where) > 0 {
		q += " WHERE " + where
	}
	results := make([]map[string]interface{}, 0)
	rows, err := table.DB.Queryx(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		row := make(map[string]interface{})
		err = rows.MapScan(row)
		for k, v := range row {
			switch v.(type) {
			case []byte:
				var jsonMap map[string]*json.RawMessage
				err := json.Unmarshal(v.([]byte), &jsonMap)
				if err != nil {
					row[k] = string(v.([]byte))
				} else {
					row[k] = jsonMap
				}
			}
		}
		results = append(results, row)
	}
	return results, nil
}

// GetMap get one item form db and return as map[string]interfaces
func (table *SchemaTable) GetMap(q string) (map[string]interface{}, error) {
	results, err := table.SelectMap(q)
	if err != nil {
		return nil, err
	}
	lenResults := len(results)
	if lenResults > 1 {
		return nil, errors.New("multiple results for GetMap query not allowed")
	} else if lenResults == 0 {
		return nil, nil
	} else {
		return results[0], nil
	}
}

// TransactCheckInsert execute insert sql string if not exist where expression in transaction
func (table *SchemaTable) TransactCheckInsert(data interface{}, where *string, query *dbc.Query, options ...map[string]interface{}) (sql.Result, error) {
	return table.checkInsert(data, where, query, options...)
}

// CheckInsert execute insert sql string if not exist where expression
func (table *SchemaTable) CheckInsert(data interface{}, where *string, options ...map[string]interface{}) (sql.Result, error) {
	return table.checkInsert(data, where, nil, options...)
}

// checkInsert execute insert sql string if not exist where expression
func (table *SchemaTable) checkInsert(data interface{}, where *string, query *dbc.Query, options ...map[string]interface{}) (sql.Result, error) {
	if query == nil {
		query = table.NewQuery()
	}
	var diff, diffPub map[string]interface{}
	idField, _, err := table.getIDField("", options)
	if err != nil {
		return nil, err
	}
	rec := reflect.ValueOf(data)
	recType := rec.Type()

	if recType.Kind() == reflect.Ptr {
		rec = reflect.Indirect(rec)
		recType = rec.Type()
	}
	var fields, values, itemID string
	var args []interface{}
	if recType.Kind() == reflect.Map {
		dataMap, ok := data.(map[string]interface{})
		if !ok {
			return nil, errors.New("element must be a map[string]interface")
		}
		args, values, fields, itemID, diff, diffPub = table.prepareArgsMap(dataMap, nil, idField)
	} else if recType.Kind() == reflect.Struct {
		args, values, fields, itemID, diff, diffPub = table.prepareArgsStruct(rec, nil, idField)
	} else {
		return nil, errors.New("element must be struct or map[string]interface")
	}

	sql := `INSERT INTO "` + table.Name + `" (` + fields + `)`
	if where == nil {
		sql += ` VALUES (` + values + `)`
	} else {
		sql += ` SELECT ` + values + ` WHERE NOT EXISTS(SELECT * FROM "` + table.Name + `" WHERE ` + *where + `)`
	}

	res, err := query.Exec(sql, args...)
	if err == nil {
		go amqp.SendUpdate(amqpURI, table.Name, itemID, "create", diffPub, options...)
		table.SaveLog(itemID, diff, options)
	}
	return res, err
}

// SaveLog save model logs to database
func (table *SchemaTable) SaveLog(itemID string, diff map[string]interface{}, options []map[string]interface{}) error {
	withLog := table.LogChanges
	if len(options) > 0 {
		option := options[0]
		if option["withLog"] != nil {
			withLog = option["withLog"].(bool)
		}
		if withLog {
			id := itemID
			user := ""
			if option["idField"] != nil {
				id = option["idField"].(string)
			}
			if option["user"] != nil {
				user = option["user"].(string)
			}
			query, err := dbc.NewQuery(false)
			if err != nil {
				return err
			}
			query.SaveLog(table.Name, id, user, diff)
		}
	}
	return nil
}

// TransactUpdateMultiple execute update sql string in transaction
func (table *SchemaTable) TransactUpdateMultiple(oldData, data interface{}, where string, query *dbc.Query, options ...map[string]interface{}) (diff, diffPub map[string]interface{}, ids []string, err error) {
	return table.updateMultiple(oldData, data, where, query, options...)
}

// UpdateMultiple execute update sql string
func (table *SchemaTable) UpdateMultiple(oldData, data interface{}, where string, options ...map[string]interface{}) (diff, diffPub map[string]interface{}, ids []string, err error) {
	return table.updateMultiple(oldData, data, where, nil, options...)
}

// updateMultiple execute update sql string
func (table *SchemaTable) updateMultiple(oldData, data interface{}, where string, query *dbc.Query, options ...map[string]interface{}) (diff, diffPub map[string]interface{}, ids []string, err error) {
	if query == nil {
		query = table.NewQuery()
	}
	rec := reflect.ValueOf(data)
	recType := rec.Type()

	if recType.Kind() == reflect.Ptr {
		rec = reflect.Indirect(rec)
		recType = rec.Type()
	}
	var fields, values string
	var args []interface{}
	if recType.Kind() == reflect.Map {
		dataMap, ok := data.(map[string]interface{})
		if !ok {
			return nil, nil, nil, errors.New("data must be a map[string]interface")
		}
		oldDataMap, ok := oldData.(map[string]interface{})
		if !ok {
			return nil, nil, nil, errors.New("oldData must be a map[string]interface")
		}
		args, values, fields, _, diff, diffPub = table.prepareArgsMap(dataMap, oldDataMap, "", options...)
	} else if recType.Kind() == reflect.Struct {
		oldRec := reflect.ValueOf(oldData)
		oldRecType := oldRec.Type()

		if oldRecType.Kind() == reflect.Ptr {
			oldRec = reflect.Indirect(oldRec)
			oldRecType = oldRec.Type()
		}
		//if oldData is map and rec is struct - convert rec to map
		if oldRecType.Kind() == reflect.Map {
			str := dbc.GetMapFromStruct(data, map[string]interface{}{"mapToJson": false})
			args, values, fields, _, diff, diffPub = table.prepareArgsMap(str, oldData.(map[string]interface{}), "", options...)
		} else {
			args, values, fields, _, diff, diffPub = table.prepareArgsStruct(rec, oldData, "", options...)
		}
	} else {
		return nil, nil, nil, errors.New("element must be struct or map[string]interface")
	}

	var sql string
	lenDiff := len(diff)
	if lenDiff == 1 {
		sql = `UPDATE "` + table.Name + `" SET ` + fields + ` = ` + values + ` WHERE ` + where + ` RETURNING id`
	} else if lenDiff > 0 {
		sql = `UPDATE "` + table.Name + `" SET (` + fields + `) = (` + values + `) WHERE ` + where + ` RETURNING id`
	}
	ids = []string{}
	if lenDiff > 0 {
		err = query.Select(&ids, sql, args...)
	}
	return diff, diffPub, ids, err
}

// TransactUpdate update one item by id
func (table *SchemaTable) TransactUpdate(id string, data interface{}, query *dbc.Query, options ...map[string]interface{}) error {
	return table.update(id, data, query, options...)
}

// Update update one item by id
func (table *SchemaTable) Update(id string, data interface{}, options ...map[string]interface{}) error {
	return table.update(id, data, nil, options...)
}

// update update one item by id
func (table *SchemaTable) update(id string, data interface{}, query *dbc.Query, options ...map[string]interface{}) error {
	idField, id, err := table.getIDField(id, options)
	if err != nil {
		return err
	}
	var oldData map[string]interface{}
	if len(options) > 0 {
		if _, ok := options[0]["oldData"]; ok {
			oldData = options[0]["oldData"].(map[string]interface{})
		}
	}
	where := `"` + idField + `"='` + id + `'`
	if oldData == nil {
		oldData, err = table.GetMap(where)
		if err != nil {
			return err
		}
	}
	diff, diffPub, _, err := table.updateMultiple(oldData, data, where, query, options...)
	if err == nil && len(diff) > 0 {
		go amqp.SendUpdate(amqpURI, table.Name, id, "update", diffPub, options...)
		table.SaveLog(id, diff, options)
	}
	return err
}

// TransactDeleteMultiple  delete all records with where sql string in transaction
func (table *SchemaTable) TransactDeleteMultiple(where string, query *dbc.Query, options ...map[string]interface{}) (int, error) {
	return table.deleteMultiple(where, query, options...)
}

// DeleteMultiple  delete all records with where sql string
func (table *SchemaTable) DeleteMultiple(where string, options ...map[string]interface{}) (int, error) {
	return table.deleteMultiple(where, nil, options...)
}

// DeleteMultiple  delete all records with where sql string
func (table *SchemaTable) deleteMultiple(where string, query *dbc.Query, options ...map[string]interface{}) (int, error) {
	if query == nil {
		query = table.NewQuery()
	}
	sql := `DELETE FROM "` + table.Name + `"`
	if len(where) > 0 {
		sql += " WHERE " + where
	}
	sql += " RETURNING id"
	var args []interface{}
	if len(options) > 0 {
		option := options[0]
		if option["args"] != nil {
			args = option["args"].([]interface{})
		}
	}
	// ret, err := q.Exec(sql, args...)
	ids := []string{}
	err := query.Select(&ids, sql, args...)
	if err == nil {
		countDelete := len(ids)
		if countDelete > 0 {
			go amqp.SendUpdate(amqpURI, table.Name, strings.Join(ids, ","), "delete", nil, options...)
		}

		return countDelete, err
	}
	return -1, err
}

// TransactDelete delete one record by id in transaction
func (table *SchemaTable) TransactDelete(id string, query *dbc.Query, options ...map[string]interface{}) (int, error) {
	return table.delete(id, query, options...)
}

// Delete delete one record by id
func (table *SchemaTable) Delete(id string, options ...map[string]interface{}) (int, error) {
	return table.delete(id, nil, options...)
}

// delete delete one record by id
func (table *SchemaTable) delete(id string, query *dbc.Query, options ...map[string]interface{}) (int, error) {
	idField, id, err := table.getIDField(id, options)
	if err != nil {
		return 0, err
	}
	count, err := table.deleteMultiple(idField+`='`+id+`'`, query, options...)
	if count != 1 {
		err = errors.New("record not found")
	}
	if err == nil {
		var data interface{}
		if len(options) > 0 {
			data = options[0]["data"]
		}
		if data == nil {
			data = map[string]interface{}{
				"id": id,
			}
		}
		go amqp.SendUpdate(amqpURI, table.Name, id, "delete", data, options...)
		//table.SaveLog(id, diff, options)
	}
	return count, err
}

// NewQuery Constructor for creating a pointer to work with the base
func (table *SchemaTable) NewQuery() (q *dbc.Query) {
	query, err := dbc.NewDBQuery(table.DB, false)
	if err != nil {
		return query
	}
	return query
}

// BeginTransaction Constructor for creating a pointer to work with the base and begin new transaction
func (table *SchemaTable) BeginTransaction() (q *dbc.Query, err error) {
	return dbc.NewDBQuery(table.DB, false)
}

// ExecQuery exec query and run callback with query result
func (table *SchemaTable) ExecQuery(queryString *string, cb ...func(rows *sqlx.Rows) bool) *dbc.QueryResult {
	return dbc.ExecQuery(queryString, cb...)
}

//UpdateStructValues update helper with nodejs mysql style format
//example UPDATE thing SET ? WHERE id = 123
func (table *SchemaTable) UpdateStructValues(queryObj *dbc.Query, query string, structVal interface{}, options ...interface{}) error {
	resultMap := make(map[string]interface{})
	oldMap := make(map[string]interface{})
	prepFields := make([]string, 0)
	prepValues := make([]string, 0)
	diff := make(map[string]interface{})
	diffPub := make(map[string]interface{})
	cntAuto := 0

	iValOld := reflect.ValueOf(structVal).Elem()
	oldModel := iValOld.FieldByName("OldModel")
	if oldModel.IsValid() {
		iVal := oldModel.Elem()
		if iVal.IsValid() {
			typ := iVal.Type()
			for i := 0; i < typ.NumField(); i++ {
				f := iVal.Field(i)
				if f.Kind() == reflect.Ptr {
					f = reflect.Indirect(f)
				}
				if !f.IsValid() {
					continue
				}
				ft := typ.Field(i)
				tag := ft.Tag.Get("db")
				tagWrite := ft.Tag.Get("dbField")
				if tagWrite == "-" || tag == "-" ||
					(tag == "" && tagWrite == "") {
					continue
				}
				if tagWrite != "" && tagWrite[0] != '-' {
					tag = tagWrite
				}
				switch val := f.Interface().(type) {
				case bool:
					oldMap[tag] = f.Bool()
				case int, int8, int16, int32, int64:
					oldMap[tag] = f.Int()
				case uint, uint8, uint16, uint32, uint64:
					oldMap[tag] = f.Uint()
				case float32, float64:
					oldMap[tag] = f.Float()
				case []byte:
					v := string(f.Bytes())
					oldMap[tag] = v
				case string:
					oldMap[tag] = f.String()
				case time.Time:
					oldMap[tag] = val.Format(time.RFC3339Nano)
				default:
					valJSON, _ := json.Marshal(val)
					oldMap[tag] = string(valJSON)
				}
			}
		}
	}

	iVal := reflect.ValueOf(structVal).Elem()
	typ := iVal.Type()
	for i := 0; i < iVal.NumField(); i++ {
		f := iVal.Field(i)
		isPointer := false
		if f.Kind() == reflect.Ptr {
			f = reflect.Indirect(f)
			isPointer = true
		}
		ft := typ.Field(i)
		tag := ft.Tag.Get("db")
		tagWrite := ft.Tag.Get("dbField")
		if tagWrite == "-" || tag == "-" ||
			(tag == "" && tagWrite == "") {
			continue
		}
		auto := false
		log := true
		if tagWrite != "" {
			if tagWrite[0] != '-' {
				tag = tagWrite
			} else if tagWrite == "-auto" {
				auto = true
			} else if tagWrite == "-nolog" {
				log = false
			}
		}
		if !f.IsValid() {
			if isPointer && oldMap[tag] != nil {
				resultMap[tag] = nil
				diff[tag] = nil
				diffPub[tag] = nil
				prepFields = append(prepFields, `"`+tag+`"`)
				prepValues = append(prepValues, "NULL")
			}
			continue
		}

		var updV string
		var rawValue interface{}

		switch val := f.Interface().(type) {
		case bool:
			resultMap[tag] = f.Bool()
			updV = strconv.FormatBool(resultMap[tag].(bool))
		case int, int8, int16, int32, int64:
			resultMap[tag] = f.Int()
			updV = fmt.Sprintf("%d", resultMap[tag])
		case uint, uint8, uint16, uint32, uint64:
			resultMap[tag] = f.Uint()
			updV = fmt.Sprintf("%d", resultMap[tag])
		case float32, float64:
			resultMap[tag] = f.Float()
			updV = fmt.Sprintf("%f", resultMap[tag])
		case []byte:
			v := string(f.Bytes())
			resultMap[tag] = v
			updV = resultMap[tag].(string)
		case string:
			resultMap[tag] = f.String()
			updV = resultMap[tag].(string)
		case time.Time:
			resultMap[tag] = val.Format(time.RFC3339Nano)
			updV = resultMap[tag].(string)
		default:
			rawValue = val
			valJSON, _ := json.Marshal(val)
			resultMap[tag] = string(valJSON)
			updV = string(valJSON)
		}

		if oldMap[tag] != resultMap[tag] {
			prepFields = append(prepFields, `"`+tag+`"`)
			prepValues = append(prepValues, "'"+updV+"'")
			if log {
				tagVal := resultMap[tag]
				diffVal := []interface{}{tagVal}
				if oldMap[tag] != nil {
					diffVal = append(diffVal, oldMap[tag])
				}
				diff[tag] = diffVal
				if rawValue != nil {
					diffPub[tag] = rawValue
				} else {
					diffPub[tag] = tagVal
				}
			}
			if auto {
				cntAuto++
			}
		}
	}
	var prepText string

	if len(prepFields) <= cntAuto {
		return nil
	} else if len(prepFields) == 1 {
		prepText = " " + strings.Join(prepFields, ",") + " = " + strings.Join(prepValues, ",") + " "
	} else {
		prepText = " (" + strings.Join(prepFields, ",") + ") = (" + strings.Join(prepValues, ",") + ") "
	}

	query = strings.Replace(query, "?", prepText, -1)
	var err error
	_, err = queryObj.Exec(query)

	if err != nil {
		logrus.Error(query)
		logrus.Error(err)
		return err
	}

	if len(options) > 0 {
		var id, user string
		withLog := true
		table := csxutils.LowerFirst(typ.Name())
		opts, ok := options[0].([]string)
		if !ok {
			optsMap, okMap := options[0].(map[string]string)
			if okMap {
				id = optsMap["id"]
				user = optsMap["user"]
				if optsMap["table"] != "" {
					table = optsMap["table"]
				}
				if optsMap["withLog"] == "false" || optsMap["withLog"] == "0" {
					withLog = false
				}
			} else {
				logrus.Error("err get info for log model info:", options)
			}
		} else if len(opts) > 1 {
			// 0 - id
			// 1 - user
			// 2 - table name
			if len(opts) > 2 {
				table = opts[2]
			}
			id = opts[0]
			user = opts[1]
		}

		if table != "" && id != "" {
			go amqp.SendUpdate(amqpURI, table, id, "update", diffPub)
			applyAmqpUpdates(table, id, queryObj)
			if withLog {
				queryObj.SaveLog(table, id, user, diff)
			}
		}
	}
	return nil
}

//InsertStructValues update helper with nodejs mysql style format
//example UPDATE thing SET ? WHERE id = 123
func (table *SchemaTable) InsertStructValues(queryObj *dbc.Query, query string, structVal interface{}, options ...interface{}) error {
	resultMap := make(map[string]interface{})
	diffPub := make(map[string]interface{})
	prepFields := make([]string, 0)
	prepValues := make([]string, 0)

	iVal := reflect.ValueOf(structVal).Elem()
	typ := iVal.Type()
	for i := 0; i < iVal.NumField(); i++ {
		f := iVal.Field(i)
		if f.Kind() == reflect.Ptr {
			f = reflect.Indirect(f)
		}
		if !f.IsValid() {
			continue
		}
		tag := typ.Field(i).Tag.Get("db")
		if tag == "-" {
			continue
		}
		tagWrite := typ.Field(i).Tag.Get("dbField")
		if tagWrite == "-" || tag == "-" ||
			(tag == "" && tagWrite == "") {
			continue
		}
		if tagWrite != "" && tagWrite[0] != '-' {
			tag = tagWrite
		}
		switch val := f.Interface().(type) {
		case int, int8, int16, int32, int64:
			resultMap[tag] = f.Int()
			diffPub[tag] = resultMap[tag]
		case uint, uint8, uint16, uint32, uint64:
			resultMap[tag] = f.Uint()
			diffPub[tag] = resultMap[tag]
		case float32, float64:
			resultMap[tag] = f.Float()
			diffPub[tag] = resultMap[tag]
		case []byte:
			v := string(f.Bytes())
			resultMap[tag] = v
			diffPub[tag] = resultMap[tag]
		case string:
			resultMap[tag] = f.String()
			diffPub[tag] = resultMap[tag]
		case time.Time:
			resultMap[tag] = val.Format(time.RFC3339Nano)
			diffPub[tag] = resultMap[tag]
		default:
			valJSON, _ := json.Marshal(val)
			resultMap[tag] = string(valJSON)
			diffPub[tag] = val
		}
		prepFields = append(prepFields, `"`+tag+`"`)
		prepValues = append(prepValues, ":"+tag)
	}

	prepText := " (" + strings.Join(prepFields, ",") + ") VALUES (" + strings.Join(prepValues, ",") + ") "

	query = strings.Replace(query, "?", prepText, -1)
	var err error
	queryObj.NamedExec(query, resultMap)
	if err != nil {
		logrus.Error(query)
		logrus.Error(err)
		return err
	}
	if len(options) > 0 {
		settings, ok := options[0].(map[string]string)
		if !ok {
			logrus.Error("err parse opt:", options)
			return nil
		}
		if len(settings) < 2 {
			logrus.Error("amqp updates, invalid args:", settings)
			return nil
		}
		table := settings["table"]
		id := settings["id"]
		go amqp.SendUpdate(amqpURI, table, id, "create", diffPub)
		registerSchema.RLock()
		schemaTableReg, ok := registerSchema.tables[table]
		registerSchema.RUnlock()
		if ok {
			if schemaTableReg.table != nil && schemaTableReg.table.getAmqpUpdateData != nil {
				for routingKey, dataCallback := range schemaTableReg.table.getAmqpUpdateData {
					updateCallback := func() {
						data := dataCallback(id)
						go amqp.SendUpdate(amqpURI, routingKey, id, "create", data)
					}
					if queryObj.IsTransact() {
						queryObj.BindTxCommitCallback(updateCallback)
					} else {
						updateCallback()
					}
				}
			}
		}

	}
	return nil
}

func Init() {
	if moduleInited {
		return
	}
	dbc.Init()
	databases := map[string]*dbc.DatabaseParams{}
	if sqlURIS != "" {
		err := json.Unmarshal([]byte(sqlURIS), &databases)
		if err != nil {
			logrus.Error("SQL_URIS env var parse failed, err:", err)
			return
		}
	}

	// TODO:: Need to deal with the default parameters
	//db.DB.SetMaxIdleConns(10)  // The default is defaultMaxIdleConns (= 2)
	//db.DB.SetMaxOpenConns(100)  // The default is 0 (unlimited)
	//db.DB.SetConnMaxLifetime(3600 * time.Second)  // The default is 0 (connections reused forever)

	registerSchema.connect(databases)
	registerSchema.prepare()
	logrus.Info("success prepare schemas")
	moduleInited = true
}
