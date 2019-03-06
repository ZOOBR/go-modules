package templater

import (
	"bytes"
	"text/template"
)

func GenTextTemplate(tpl *string, data interface{}) string {
	var gen bytes.Buffer
	t := template.Must(template.New("").Parse(*tpl))
	t.Execute(&gen, data)
	return gen.String()
}
