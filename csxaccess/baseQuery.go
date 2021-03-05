package csxaccess

import (
	"net/http"
	"strings"

	"gitlab.com/battler/modules/csxerrors"
	"gitlab.com/battler/modules/csxhttp"
	dbc "gitlab.com/battler/modules/sql"
)

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

// PrepareBaseQuery prepares base SQL query
func PrepareBaseQuery(ctx *csxhttp.Context, mode int, params map[string]string) (*BaseQuery, *dbc.SchemaTable, *string, map[string]string, int, string) {
	// values := ctx.Values()
	route := ctx.Request().URL.RequestURI()
	status, ok := ctx.GetInt("status")
	if !ok {
		status = 0
	}
	isSuperUser := status == 1
	// if values == nil {
	// 	return nil, nil, nil, nil, csxerrors.Error(ctx, "MethodNotAllowed", "method not allowed ", route), ""
	// }

	baseQ := BaseQuery{}
	if mode > QueryModeRead {
		err := ctx.Bind(&baseQ)
		if err != nil || baseQ.Collection == "" {
			csxerrors.Error(ctx, "MissingReqParams", "invalid body params ", route)
			return nil, nil, nil, nil, ctx.Response().Status, ""
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
		csxerrors.Error(ctx, "MissingReqParams", "empty collection ", route)
		return nil, nil, nil, nil, ctx.Response().Status, ""
	}

	schema, _ := dbc.GetSchemaTable(baseQ.Collection)
	baseQ.Schema = schema

	userRoles := ctx.Get("roles").(map[string]interface{})

	// TODO:: Rework all tables to schema table
	isAccess := baseQ.StrictAccess(mode, fields, userRoles, isSuperUser)
	if baseQ.StrictFields != nil && params != nil {
		params["fields"] = strings.Join(*baseQ.StrictFields, ",")
	}
	if !isAccess {
		msg := `Collection "` + baseQ.Collection + `" not allowed`
		csxerrors.Error(ctx, msg, msg, route)
		return nil, nil, nil, nil, ctx.Response().Status, ""
	}

	userID := ctx.Get("id").(string)
	restrictAccessCondition := ""
	if schema != nil {
		restrictAccessCondition = schema.RestrictRolesRights(GetRolesRights(userRoles))
	}
	return &baseQ, schema, &userID, params, http.StatusOK, restrictAccessCondition
}

// PrepareQuery prepares SQL query
func PrepareQuery(ctx *csxhttp.Context, mode int, params map[string]string) (map[string]string, int, string) {
	_, _, _, params, status, restrictAccessCondition := PrepareBaseQuery(ctx, mode, params)
	return params, status, restrictAccessCondition
}
