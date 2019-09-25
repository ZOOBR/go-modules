package templater

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"text/template"

	log "github.com/sirupsen/logrus"
	dbc "gitlab.com/battler/modules/sql"
)

// MsgTemplate structure of record msgTemplate table
type MsgTemplate struct {
	ID        string `db:"id" json:"id"`
	Name      string `db:"name" json:"name"`
	Template  string `db:"template" json:"template"`
	templates map[string]*template.Template
}

var msgTemplates = make(map[string]*MsgTemplate)

var msgTemplate = dbc.CreateSchemaTable("msgTemplate",
	dbc.CreateSchemaField("id", "varchar", 50, true),
	dbc.CreateSchemaField("name", "varchar", 50),
	dbc.CreateSchemaField("template", "jsonb"),
)

func prepareTemplate(id string) *MsgTemplate {
	mt := msgTemplates[id]
	if mt != nil {
		return mt
	}
	mt = &MsgTemplate{}
	mt.templates = make(map[string]*template.Template)
	msgTemplates[id] = mt
	err := msgTemplate.Get(mt, `id = '`+id+`'`)
	if err != nil {
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
					msg := string(buffer.Bytes())
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
	t := mt.templates[lang]
	if t == nil && lang != "en" {
		t = mt.templates["en"]
	}
	if t == nil {
		return "", errors.New("template not found")
	}
	var gen bytes.Buffer
	err := t.Execute(&gen, data)
	return gen.String(), err
}

// Format message by template id
func Format(id, lang string, data interface{}) (string, error) {
	return prepareTemplate(id).format(lang, data)
}

// GenTextTemplate generate message from template string
func GenTextTemplate(tpl *string, data interface{}) string {
	var gen bytes.Buffer
	t := template.Must(template.New("").Parse(*tpl))
	t.Execute(&gen, data)
	return gen.String()
}

type test struct {
	Name, Gift string
	Attended   bool
}

// Init is module initialiser
func Init() {
	/* test formats
	msg, err := Format("test", "en", test{
		"name", "test", false,
	})
	log.Debug(msg, err)
	msg, err = Format("test", "en", test{
		"name2", "test2", false,
	})
	log.Debug(msg, err)
	//*/
}
