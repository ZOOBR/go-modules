package templater

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"text/template"

	log "github.com/sirupsen/logrus"
	dbc "gitlab.com/battler/modules/sql"
)

type msgTemplateReg struct {
	id        string
	templates map[string]*template.Template
}

var msgTemplates struct {
	list  map[string]*msgTemplateReg
	mutex sync.RWMutex
}

// MsgTemplate structure of record msgTemplate table
type MsgTemplate struct {
	ID       string `db:"id" json:"id" len:"50" key:"1"`
	Name     string `db:"name" json:"name" len:"50"`
	Template string `db:"template" json:"template" type:"jsonb"`
}

var msgTemplate = dbc.NewSchemaTable("msgTemplate", MsgTemplate{}, map[string]interface{}{
	"onUpdate": func(table *dbc.SchemaTable, msg interface{}) {
		msgTemplates.mutex.Lock()
		msgTemplates.list = make(map[string]*msgTemplateReg)
		msgTemplates.mutex.Unlock()
		log.Debug("MsgTemplate [onUpdate] clear templates")
	},
})

func prepareTemplate(id string) *msgTemplateReg {
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
	reg.id = id
	reg.templates = make(map[string]*template.Template)
	if id[0] == '#' {
		id = id[1:]
	}
	var mt MsgTemplate
	err := msgTemplate.Get(&mt, `id = '`+id+`'`)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Error("MsgTemplate [prepareTemplate] Error load '"+id+"' ", err)
		} else {
			log.Error("MsgTemplate [prepareTemplate] Not found template '" + id + "' ")
		}
	} else {
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
				if err == nil {
					msg := ""
					bytes := buffer.Bytes()
					lenBytes := len(bytes)
					offsetBytes := 0
					if lenBytes > 0 {
						if bytes[lenBytes-1] == 10 {
							lenBytes--
						}
						if bytes[0] == 34 && bytes[lenBytes-1] == 34 {
							offsetBytes = 1
							lenBytes--
						}
						msg = string(bytes[offsetBytes:lenBytes])
					}
					t, err := template.New(mt.ID).Parse(msg)
					if err == nil {
						reg.templates[key] = t
					} else {
						log.Error("MsgTemplate [prepareTemplate] Error parse '"+id+"' '"+key+"' ", err)
					}
				}
			}
		}
	}

	msgTemplates.mutex.Lock()
	if msgTemplates.list == nil {
		msgTemplates.list = make(map[string]*msgTemplateReg)
	}
	msgTemplates.list[id] = reg
	msgTemplates.mutex.Unlock()
	return reg
}

func (reg *msgTemplateReg) format(lang string, data interface{}) (string, error) {
	var err error
	var text string
	var t *template.Template
	if len(lang) > 0 {
		t = reg.templates[lang]
	}
	if t == nil && lang != "en" {
		t = reg.templates["en"]
	}
	if t == nil {
		err = errors.New("template not found")
	} else {
		var gen bytes.Buffer
		err = t.Execute(&gen, data)
		text = gen.String()
	}
	if len(text) == 0 {
		text = "[" + reg.id + "]"
	}
	return text, err
}

func format(id, lang string, data interface{}) (string, error) {
	var text string
	var err error
	if len(id) == 0 {
		text = ""
	} else if id[0] != '#' {
		text = id
	} else {
		text, err = prepareTemplate(id).format(lang, data)
	}
	return text, err
}

// Format message by template id with map or struct data
// Template string: "Hello <b>{{.Name}}</b> {{.Caption}}"
func Format(id, lang string, data interface{}) (string, error) {
	return format(id, lang, data)
}

// FormatParams message by template id with unnamed parameters
// Template string: "Hello <b>{{.p0}}</b> {{.p1}}"
func FormatParams(id, lang string, params ...interface{}) (string, error) {
	data := map[string]interface{}{}
	for index, param := range params {
		data["p"+strconv.Itoa(index)] = param
	}
	return format(id, lang, data)
}

// GenTextTemplate generate message from template string
func GenTextTemplate(tpl *string, data interface{}) string {
	var gen bytes.Buffer
	t := template.Must(template.New("").Parse(*tpl))
	t.Execute(&gen, data)
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
	msg, err := Format("#test", "en", struct{ Name, Gift string }{
		"name", "test",
	})
	log.Debug(msg, err)

	// map data
	msg, err = Format("#test", "en", map[string]interface{}{
		"Name": "name2",
		"Gift": "test2",
	})
	log.Debug(msg, err)

	// unamed parameters
	msg, err = FormatParams("#auth.code.message", "en", 121343)
	log.Debug(msg, err)

	//*/
}
