package templater

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"text/template"

	log "github.com/sirupsen/logrus"
	dbc "gitlab.com/battler/modules/sql"
)

type msgTemplateReg struct {
	id        string
	typ       string
	templates map[string]*template.Template
}

var msgTemplates struct {
	list  map[string]*msgTemplateReg
	mutex sync.RWMutex
}

var defaultLocale = os.Getenv("CLIENT_DEFAULT_LOCALE")

// MsgTemplate structure of record msgTemplate table
type MsgTemplate struct {
	ID       string `db:"id" json:"id" len:"50" key:"1"`
	Name     string `db:"name" json:"name" len:"50"`
	Template string `db:"template" json:"template" type:"jsonb"`
}

var MsgTemplateSchema = dbc.NewSchemaTable("msgTemplate", MsgTemplate{}, map[string]interface{}{
	"onUpdate": func(table *dbc.SchemaTable, msg interface{}) {
		msgTemplates.mutex.Lock()
		msgTemplates.list = make(map[string]*msgTemplateReg)
		msgTemplates.mutex.Unlock()
		log.Debug("MsgTemplate [onUpdate] clear templates")
	},
})

func prepareTemplate(id string, isTemplate bool) *msgTemplateReg {
	var reg *msgTemplateReg
	msgTemplates.mutex.RLock()
	if msgTemplates.list != nil {
		reg = msgTemplates.list[id]
	}
	msgTemplates.mutex.RUnlock()
	if reg != nil {
		return reg
	}
	reg = &msgTemplateReg{}
	var mt MsgTemplate
	reg.templates = make(map[string]*template.Template)
	if isTemplate {
		reg.id = id
		err := MsgTemplateSchema.Get(&mt, `id = '`+id+`'`)
		if err != nil {
			if err != sql.ErrNoRows {
				log.Error("MsgTemplate [prepareTemplate] Error load '"+id+"' ", err)
			} else {
				log.Error("MsgTemplate [prepareTemplate] Not found template '" + id + "' ")
			}
		}
	} else {
		mt = MsgTemplate{Template: id}
	}
	var objmap map[string]*json.RawMessage
	err := json.Unmarshal([]byte(mt.Template), &objmap)
	if err != nil {
		log.Error("MsgTemplate [prepareTemplate] Error parse '"+id+"' json ", err)
	} else {
		for key, val := range objmap {
			buffer := &bytes.Buffer{}
			encoder := json.NewEncoder(buffer)
			encoder.SetEscapeHTML(false)
			err := encoder.Encode(val)
			if err != nil {
				log.Error("MsgTemplate [prepareTemplate] Error parse '"+id+"' '"+key+"' ", err)
				continue
			}
			var str string
			bytes := buffer.Bytes()
			lenBytes := len(bytes)
			if lenBytes > 0 {
				if bytes[lenBytes-1] == 10 {
					lenBytes--
				}
				str = string(bytes[0:lenBytes])
				uqstr, err := strconv.Unquote(str)
				if err == nil {
					str = uqstr
				} else if bytes[0] == 34 && bytes[lenBytes-1] == 34 {
					str = string(bytes[1 : lenBytes-1])
				}
			}
			if key == "type" {
				reg.typ = str
			} else {
				t, err := template.New(mt.ID).Option("missingkey=zero").Parse(str)
				if err == nil {
					reg.templates[key] = t
				} else {
					log.Error("MsgTemplate [prepareTemplate] Error parse '"+id+"' '"+key+"' ", err)
				}
			}
		}
	}

	msgTemplates.mutex.Lock()
	if msgTemplates.list == nil {
		msgTemplates.list = make(map[string]*msgTemplateReg)
	}
	if isTemplate {
		msgTemplates.list[id] = reg
	}
	msgTemplates.mutex.Unlock()
	return reg
}

// correctDataForExec convert struct to map, needed for zero on not found
func correctDataForExec(data interface{}) interface{} {
	rec := reflect.ValueOf(data)
	if !rec.IsValid() {
		return data
	}
	recType := reflect.TypeOf(data)

	if recType.Kind() == reflect.Ptr {
		rec = reflect.ValueOf(data).Elem()
		recType = reflect.TypeOf(data)
	}
	if recType.Kind() != reflect.Struct {
		return data
	}
	m := map[string]interface{}{}
	fcnt := recType.NumField()
	for i := 0; i < fcnt; i++ {
		f := recType.Field(i)
		v := reflect.Indirect(rec.FieldByName(f.Name))
		if v.IsValid() {
			m[f.Name] = reflect.Indirect(rec.FieldByName(f.Name))
		}
	}
	return m
}

func (reg *msgTemplateReg) format(lang string, data interface{}) (string, string, error) {
	var err error
	var text, typ string
	var t *template.Template

	if lang == "" {
		lang = defaultLocale
	}

	t = reg.templates[lang]
	if t == nil && lang != "en" {
		t = reg.templates["en"]
	}
	if t == nil {
		err = errors.New("template not found")
	} else {
		var gen bytes.Buffer
		err = t.Execute(&gen, correctDataForExec(data))
		text = strings.Replace(gen.String(), "<no value>", "", -1)
	}
	if len(text) == 0 {
		text = "[" + reg.id + "]"
	}
	return text, typ, err
}

func format(id, lang string, isTemplate bool, data interface{}) (string, string, error) {
	var text, typ string
	var err error
	if len(id) > 0 {
		text, typ, err = prepareTemplate(id, isTemplate).format(lang, data)
	}
	return text, typ, err
}

// Format message by template id with map or struct data
// Template string: "Hello <b>{{.Name}}</b> {{.Caption}}"
func Format(id, lang string, data interface{}, options ...map[string]interface{}) (string, string, error) {
	isTemplate := true
	if len(options) > 0 {
		if tmplOption, ok := options[0]["isTemplate"]; ok {
			if tmpl, ok := tmplOption.(bool); ok {
				isTemplate = tmpl
			} else {
				log.Error("Error format isTemplate: " + id)
			}
		}
	}
	return format(id, lang, isTemplate, data)
}

// FormatParams message by template id with unnamed parameters
// Template string: "Hello <b>{{.p0}}</b> {{.p1}}"
func FormatParams(id, lang string, params ...interface{}) (string, string, error) {
	data := map[string]interface{}{}
	for index, param := range params {
		data["p"+strconv.Itoa(index)] = param
	}
	return format(id, lang, true, data)
}

// GenTextTemplate generate message from template string
func GenTextTemplate(tpl *string, data interface{}) string {
	var gen bytes.Buffer
	t := template.Must(template.New("").Parse(*tpl))
	t.Execute(&gen, correctDataForExec(data))
	return gen.String()
}

// Init is module initialization
func Init() {
	//* test formats

	// insert test template
	// msgTemplate.Insert(MsgTemplate{
	// 	"test",
	// 	"test message",
	// 	`{"en": "Hello <b>{{.Name}}</b> {{.Gift}}", "ru": "Привет <b>{{.Name}}</b>"}`,
	// })

	// structure data
	msg, _, err := Format("#test", "en", struct{ Name, Gift string }{
		"name", "test",
	})
	log.Debug(msg, err)

	// map data
	msg, _, err = Format("#test", "en", map[string]interface{}{
		"Name": "name2",
		"Gift": "test2",
	})
	log.Debug(msg, err)

	// unamed parameters
	msg, _, err = FormatParams("#auth.code.message", "en", 121343)
	log.Debug(msg, err)

	//*/
}
