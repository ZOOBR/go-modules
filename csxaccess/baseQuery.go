package csxaccess

import dbc "gitlab.com/battler/modules/sql"

// BaseQuery struct for get update, upsert and delete http queries
type BaseQuery struct {
	ID           string `json:"id"`
	Collection   string `json:"collection"`
	Schema       *dbc.SchemaTable
	StrictFields *[]string
	Data         map[string]interface{} `json:"data"`
	manager      *AccessManager
}

// StrictAccess check user access to clollection with reade, update and delete modes
func (baseQ *BaseQuery) StrictAccess(mode int, fields []string, roles map[string]interface{}, isSuperUser bool) bool {
	// If * is specified, take a list of all fields from the scheme
	// TODO:: baseQ.Schema != nil this crutch for use base info without schema
	if baseQ.Schema != nil && len(fields) == 1 && fields[0] == "*" {
		newFields := make([]string, len(baseQ.Schema.Fields))
		for key, field := range baseQ.Schema.Fields {
			newFields[key] = field.Name
		}
		fields = newFields
	}
	success, newFields := baseQ.manager.StrictAccess(baseQ.Collection, mode, fields, roles, true, isSuperUser)
	if success && newFields != nil {
		baseQ.StrictFields = &newFields
	}
	return success
}
