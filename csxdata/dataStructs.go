package csxdata

import (
	"github.com/sirupsen/logrus"
	"gitlab.com/battler/modules/csxhttp"
	"gitlab.com/battler/modules/sql"
)

//DataImport
type DataImport struct {
	Action string `json:"action"`
	Item   ModelActionsRunner
}

type OperationContext struct {
	HttpContext *csxhttp.Context
	Query       *sql.Query
}

//ModelActionRunner
type ModelActionsRunner interface {
	ActionInsert(*OperationContext)
	ActionDelete(*OperationContext)
	ActionUpdate(*OperationContext)
}

// ApplyActions
func ApplyActions(data []DataImport, ctx *OperationContext) {
	for _, item := range data {
		switch item.Action {
		case "insert":
			item.Item.ActionInsert(ctx)
		case "delete":
			item.Item.ActionDelete(ctx)
		case "update":
			item.Item.ActionUpdate(ctx)
		default:
			logrus.Debug("action case not found for rentState")
		}
	}
}
