package templater

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"text/template"

	log "github.com/sirupsen/logrus"
	dbc "gitlab.com/battler/modules/sql"
)

// MsgTemplate structure of record msgTemplate table
type MsgTemplate struct {
	ID        string `db:"id" json:"id" len:"50" key:"1"`
	Name      string `db:"name" json:"name" len:"50"`
	Template  string `db:"template" json:"template" type:"jsonb"`
	templates map[string]*template.Template
}

var msgTemplate = dbc.NewSchemaTable("msgTemplate", MsgTemplate{})
var msgTemplates = make(map[string]*MsgTemplate)

func prepareTemplate(id string) *MsgTemplate {
	mt := msgTemplates[id]
	if mt != nil {
		return mt
	}
	mt = &MsgTemplate{}

	mt.templates = make(map[string]*template.Template)
	msgTemplates[id] = mt
	if id[0] == '#' {
		id = id[1:]
	}
	err := msgTemplate.Get(mt, `id = '`+id+`'`)
	if err != nil {
		mt.ID = id
		if err != sql.ErrNoRows {
			log.Error("MsgTemplateFailed", "[prepareTemplate]", "Error load '"+id+"' ", err)
		} else {
			log.Error("MsgTemplateFailed", "[prepareTemplate]", "Not found template '"+id+"' ")
		}
	} else {
		var objmap map[string]*json.RawMessage
		err := json.Unmarshal([]byte(mt.Template), &objmap)
		if err != nil {
			log.Error("MsgTemplateFailed", "[prepareTemplate]", "Error parse '"+id+"' json ", err)
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
						mt.templates[key] = t
					} else {
						log.Error("MsgTemplateFailed", "[prepareTemplate]", "Error parse '"+id+"' '"+key+"' ", err)
					}
				}
			}
		}
	}
	return mt
}

func (mt *MsgTemplate) format(lang string, data interface{}) (string, error) {
	var err error
	var text string
	var t *template.Template
	if len(lang) > 0 {
		t = mt.templates[lang]
	}
	if t == nil && lang != "en" {
		t = mt.templates["en"]
	}
	if t == nil {
		err = errors.New("template not found")
	} else {
		var gen bytes.Buffer
		err = t.Execute(&gen, data)
		text = gen.String()
	}
	if len(text) == 0 {
		text = "[" + mt.ID + "]"
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
	/* test formats

	msgTemplate.Insert(MsgTemplate{
		"test3",
		"test2",
		"{}",
		nil,
	})

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
