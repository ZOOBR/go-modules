package csxaccess

import (
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	"gitlab.com/battler/modules/csxdatastore"
	dbc "gitlab.com/battler/modules/sql"
)

const (
	// QueryModeRead mode for read collections items
	QueryModeRead = 1
	// QueryModeCreate mode for upsert and update
	QueryModeCreate = 2
	// QueryModeUpdate mode for upsert and update
	QueryModeUpdate = 4
	// QueryModeDelete mode for delet collection items
	QueryModeDelete = 8
	// QueryModeDelete mode for read all fields exlude deny fields
	QueryModeReadWithDeny = 16
)

type AccessManager struct {
	accessMap *csxdatastore.DataStore
}

// Mandat is a base partner struct
type Mandat struct {
	ID       string
	Subject  string
	Group    string
	Category string
	Role     *string
	Fields   []string
	Priority int
	Access   int
	Params   map[string]interface{}
}

type ContextFields struct {
	Route  string
	Roles  map[string]interface{}
	Status int
	Fields string
}

// CheckAccess check manadat access
func (mandat *Mandat) CheckAccess(roles map[string]interface{}) int {
	if mandat.CheckRole(roles) {
		return mandat.Access
	}
	return 0
}

// Assign assign mandat properties
func (mandat *Mandat) Assign(extMandat *Mandat) *Mandat {
	if extMandat.Access > 0 {
		mandat.Access |= extMandat.Access
	} else if extMandat.Access < 0 {
		mandat.Access ^= extMandat.Access * -1
	}

	if extMandat.Fields != nil && mandat.Fields != nil {
		for i := 0; i < len(extMandat.Fields); i++ {
			extField := (extMandat.Fields)[i]
			isFind := false
			for j := 0; j < len(mandat.Fields); j++ {
				field := (mandat.Fields)[j]
				if field == extField {
					isFind = true
					break
				}
			}
			if !isFind {
				mandat.Fields = append(mandat.Fields, extField)
			}
		}
	} else if extMandat.Fields != nil && mandat.Fields == nil {
		mandat.Fields = extMandat.Fields
	}
	return mandat
}

// CheckMandatParams checks mandat params permission
func (mandat *Mandat) CheckMandatParams(urlParams map[string]string) bool {
	if mandat.Params == nil {
		return true
	}
	result := true
	for paramName, paramValueInt := range mandat.Params {
		paramValue, ok := paramValueInt.(string)
		if !ok {
			continue
		}

		if paramValue != urlParams[paramName] {
			result = false
			break
		}

		if !result {
			break
		}

	}
	return result
}

// CheckRole check manadat role
func (mandat *Mandat) CheckRole(roles map[string]interface{}) bool {
	_, ok := roles[*mandat.Role]
	return ok
}

func createNewFields(fields []string, newFields []string, strictFields map[string]bool, db bool) {
	idx := 0
	pattern := `^[a-zA-Z0-9]*$`
	regexpInst, err := regexp.Compile(pattern)
	if err != nil {
		logrus.Error("invalid createNewFields() pattern:" + pattern)
	}
	for _, field := range fields {
		if strictFields != nil {
			if _, ok := strictFields[field]; !ok {
				continue
			}
		}
		if db {
			// check alphanumeric
			matched := regexpInst.Match([]byte(field))
			if matched {
				newFields[idx] = `"` + field + `"`
			} else {
				newFields[idx] = field
			}
		} else {
			newFields[idx] = field
		}

		idx++
	}
}

// StrictAccess check user access to clollection with read, update and delete modes
func (manager *AccessManager) StrictAccess(subject string, mode int, fields []string, roles map[string]interface{}, db bool, isSuperUser bool) (bool, []string) {
	success := false
	if isSuperUser {
		newFields := make([]string, len(fields))
		createNewFields(fields, newFields, nil, db)
		return true, newFields
	}
	mandatsInt, ok := manager.accessMap.FindByIndex("Subject", subject)
	if !ok {
		return success, nil
	}
	mandats, ok := mandatsInt.(*sync.Map)
	if !ok {
		return success, nil
	}
	strictFields := make(map[string]bool)
	mandats.Range(func(_, mandatInt interface{}) bool {
		mandat, ok := mandatInt.(*Mandat)
		if !ok {
			return true
		}
		access := mandat.CheckAccess(roles)

		// prohibit access to fields if prohibited
		denyMode := access <= 0
		if denyMode && mandat.Fields == nil {
			return true
		}

		// mandates less than zero prohibit access
		currentAccess := int(math.Abs(float64(access)))
		if mode&currentAccess == 0 {
			return true
		}

		// There may be several mandates, they are applied in accordance with priority
		// As a result, the decision to ban or allow the operation will be behind the mandate with the highest priority
		success = true

		// If no access fields for the credential are specified, then we consider that all fields are available
		if mandat.Fields == nil {
			// set all fields
			for _, field := range fields {
				strictFields[field] = true
			}
			return true
		}

		// All fields requested for work are bypassed and fields are checked according to the mandate.
		// Deny credentials remove fields that allow add
		lenFields := len(fields)
		for i := 0; i < lenFields; i++ {
			field := (fields)[i]
			for _, mandatField := range mandat.Fields {
				// * allows all fields
				if mandatField == "*" {
					if denyMode {
						strictFields = make(map[string]bool)
						break
					}
					for _, field := range fields {
						strictFields[field] = true
					}
				}
				if mandatField == field && denyMode {
					delete(strictFields, field)
				} else if mandatField == field {
					strictFields[field] = true
				}
			}
		}
		return true
	})
	var lenFields int
	if isSuperUser {
		lenFields = len(fields)
	} else {
		lenFields = len(strictFields)
	}
	newFields := make([]string, lenFields)
	createNewFields(fields, newFields, strictFields, db)
	return success, newFields
}

func (manager *AccessManager) CheckAccessFields(ctxFields *ContextFields, defFields []string, db bool) (int, []string) {
	route := ctxFields.Route
	userRoles := ctxFields.Roles
	fieldStr := ctxFields.Fields
	status := ctxFields.Status
	isSuperUser := status == 1
	if isSuperUser && fieldStr == "" {
		return 1, defFields
	}

	var fields []string
	if fieldStr == "" {
		fields = defFields
	} else {
		fields = strings.Split(fieldStr, ",")
	}
	var access bool
	var newFields []string
	if isSuperUser {
		access = true
		newFields = fields
	} else {
		access, newFields = manager.StrictAccess(route, QueryModeRead, fields, userRoles, db, isSuperUser)
	}
	if !access || len(newFields) == 0 {
		return 0, nil
	}
	if len(newFields) == len(defFields) {
		return 2, newFields
	}
	return 1, newFields
}

// Load ---
func (manager *AccessManager) Load(mandats []*Mandat) {
	sortMandats(mandats)
	indexies := map[string]csxdatastore.DataStoreIndex{
		"ID":       {IndexType: csxdatastore.IndexTypeUnique},
		"Subject":  {IndexType: csxdatastore.IndexTypeMap},
		"Category": {IndexType: csxdatastore.IndexTypeMap},
	}
	manager.accessMap = csxdatastore.NewDataStore("mandats", "ID", indexies, func(id *string) (interface{}, error) {
		return mandats, nil
	})
	manager.accessMap.Load()
}

func NewAccessManager(accessMandats []*Mandat) *AccessManager {
	accessManager := AccessManager{}
	accessManager.Load(accessMandats)
	return &accessManager
}

func sortMandats(mandatsNew []*Mandat) (result []*Mandat) {
	sorts := []string{"role", "group", "priority"}
	for i := 0; i < len(sorts); i++ {
		sortID := sorts[i]
		sort.SliceStable(mandatsNew, func(i, j int) bool {
			elem1 := mandatsNew[i]
			elem2 := mandatsNew[j]

			if sortID == "role" {
				if elem1.Role == nil || elem2.Role == nil {
					return elem1.Role != nil
				}
				return *elem1.Role < *elem2.Role
			} else if sortID == "group" {
				if elem1.Group == "" || elem2.Group == "" {
					return elem1.Group != ""
				}
				return elem1.Group < elem2.Group
			}
			return elem1.Priority < elem2.Priority
		})
	}
	return mandatsNew
}

func (manager *AccessManager) FindMandat(id string) (*Mandat, bool) {
	mandatInt, ok := manager.accessMap.FindByIndex("Id", id)
	if !ok {
		return nil, ok
	}
	mandat, ok := mandatInt.(*Mandat)
	return mandat, ok
}

func (manager *AccessManager) AddMandat(id string, newMandat *Mandat) {
	manager.accessMap.Store(id, newMandat)
}

func (manager *AccessManager) DeleteMandats(ids []string) {
	for i := 0; i < len(ids); i++ {
		id := ids[i]
		manager.accessMap.Delete(id)
	}
}

// GetMandatsByCategory ---
func (manager *AccessManager) GetMandatsByCategory(category string) ([]*Mandat, bool) {
	mandatsInt, ok := manager.accessMap.FindByIndex("Category", category)
	if !ok {
		return nil, false
	}
	mandats, ok := mandatsInt.(*sync.Map)
	if !ok {
		return nil, true
	}
	resultMandats := []*Mandat{}
	mandats.Range(func(mandatID, mandatInt interface{}) bool {
		currentMandat, ok := mandatInt.(*Mandat)
		if !ok {
			return true
		}
		resultMandats = append(resultMandats, currentMandat)
		return true
	})
	sortMandats(resultMandats)
	return resultMandats, true
}

// GetMandatBySubject ---
func (manager *AccessManager) GetMandatBySubject(subject string, roles map[string]interface{}) (*Mandat, bool) {
	mandatsInt, ok := manager.accessMap.FindByIndex("Subject", subject)
	if !ok {
		return nil, false
	}
	mandats, ok := mandatsInt.(*sync.Map)
	if !ok {
		return nil, true
	}
	roleMandats := []*Mandat{}
	mandat := &Mandat{}
	mandats.Range(func(mandatID, mandatInt interface{}) bool {
		currentMandat, ok := mandatInt.(*Mandat)
		if !ok {
			return true
		}
		roleMandats = append(roleMandats, currentMandat)
		return true
	})
	sort.SliceStable(roleMandats, func(i, j int) bool {
		return roleMandats[i].Priority < roleMandats[j].Priority
	})
	for i := 0; i < len(roleMandats); i++ {
		currentMandat := roleMandats[i]
		if !currentMandat.CheckRole(roles) {
			continue
		}
		if mandat.ID == "" {
			mandat = currentMandat
		} else if mandat.ID != currentMandat.ID {
			mandat.Assign(currentMandat)
		}
	}
	return mandat, true
}

// GetRolesRights returns map of role interfaces from string
func GetRolesRights(roles map[string]interface{}) map[string]*dbc.JsonB {
	result := map[string]*dbc.JsonB{}
	for roleID, roleInt := range roles {
		role := roleInt.(map[string]interface{})
		rightsInt := role["rights"]
		if rightsInt == nil {
			continue
		}
		rights := dbc.JsonB(rightsInt.(map[string]interface{}))
		result[roleID] = &rights
	}
	return result
}

func (manager *AccessManager) PrintMandats() {
	manager.accessMap.Range(func(key, val interface{}) bool {
		logrus.Debug("load mandat:", key)
		return true
	})

}
