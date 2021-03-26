package templater

import (
	"bytes"
	"reflect"
	"text/template"
)

// correctDataForExec convert struct to map, needed for zero on not found
func CorrectDataForExec(data interface{}) interface{} {
	rec := reflect.ValueOf(data)
	if !rec.IsValid() {
		return data
	}
	recType := reflect.TypeOf(data)

	if recType.Kind() == reflect.Ptr {
		rec = reflect.ValueOf(data).Elem()
		recType = reflect.TypeOf(data)
	}
	if recType.Kind() == reflect.Map {
		res, ok := data.(map[string]interface{})
		if !ok /* recType.String() == "models.JsonB" */ {
			return rec.Convert(reflect.TypeOf(res))
		}
		return res
	} else if recType.Kind() != reflect.Struct {
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

// GenTextTemplate generate message from template string
func GenTextTemplate(tpl *string, data interface{}) string {
	var gen bytes.Buffer
	t := template.Must(template.New("").Parse(*tpl))
	t.Execute(&gen, CorrectDataForExec(data))
	return gen.String()
}

// Init is module initialization
func Init() {

}
