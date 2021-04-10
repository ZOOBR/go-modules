package csxhttp

import (
	"net/http"
	"strings"

	"gitlab.com/battler/modules/csxaccess"
	"gitlab.com/battler/modules/csxschema"
)

// BaseQuery struct for get update, upsert and delete http queries
type BaseQuery struct {
	ID                      string `json:"id"`
	Collection              string `json:"collection"`
	Schema                  *csxschema.SchemaTable
	StrictFields            *[]string
	Data                    map[string]interface{} `json:"data"`
	RestrictAccessCondition string
}

// StrictAccess check user access to clollection with reade, update and delete modes
func (baseQ *BaseQuery) StrictAccess(ctx *Context, mode int, fields []string) bool {
	status, ok := ctx.GetInt("status")
	if !ok {
		status = 0
	}
	isSuperUser := status == 1
	roles := ctx.Get("roles").(map[string]interface{})
	// If * is specified, take a list of all fields from the scheme
	// TODO:: baseQ.Schema != nil this crutch for use base info without schema
	if baseQ.Schema != nil && len(fields) == 1 && fields[0] == "*" {
		newFields := make([]string, len(baseQ.Schema.Fields))
		for key, field := range baseQ.Schema.Fields {
			newFields[key] = field.Name
		}
		fields = newFields
	}
	success, newFields := ctx.AccessManager().StrictAccess(baseQ.Collection, mode, fields, roles, true, isSuperUser)
	if success && newFields != nil {
		baseQ.StrictFields = &newFields
	}
	if baseQ.Schema != nil {
		baseQ.RestrictAccessCondition = baseQ.Schema.RestrictRolesRights(csxaccess.GetRolesRights(roles))
	}
	return success
}

// PrepareBaseQuery prepares base SQL query
func PrepareBaseQuery(ctx *Context, mode int, params map[string]string) (*BaseQuery, int) {
	return prepareBaseQuery(ctx, mode, params)
}

// PrepareQuery prepares SQL query
func PrepareQuery(ctx *Context, mode int, params map[string]string) (int, string) {
	baseQ, status := prepareBaseQuery(ctx, mode, params)
	if baseQ == nil {
		return status, ""
	}
	return status, baseQ.RestrictAccessCondition
}

// prepareBaseQuery prepares base SQL query
func prepareBaseQuery(ctx *Context, mode int, params map[string]string) (*BaseQuery, int) {
	route := ctx.Request().URL.RequestURI()

	baseQ := BaseQuery{}
	if mode > csxaccess.QueryModeRead {
		err := ctx.Bind(&baseQ)
		if err != nil || baseQ.Collection == "" {
			Error(ctx, "MissingReqParams", "invalid body params ", route)
			return nil, ctx.Response().Status
		}
	}
	var fields []string
	if len(params) > 0 {
		collection := params["collection"]
		if collection == "" {
			collection = params["table"]
		}
		baseQ.Collection = collection
		fields = strings.Split(params["fields"], ",")
	}

	if len(baseQ.Collection) == 0 {
		Error(ctx, "MissingReqParams", "empty collection ", route)
		return nil, ctx.Response().Status
	}

	schema, _ := csxschema.GetSchemaTable(baseQ.Collection)
	baseQ.Schema = schema

	// TODO:: Rework all tables to schema table
	isAccess := baseQ.StrictAccess(ctx, mode, fields)
	if baseQ.StrictFields != nil && params != nil {
		params["fields"] = strings.Join(*baseQ.StrictFields, ",")
	}
	if !isAccess {
		msg := `Collection "` + baseQ.Collection + `" not allowed`
		Error(ctx, msg, msg, route)
		return nil, ctx.Response().Status
	}

	return &baseQ, http.StatusOK
}
