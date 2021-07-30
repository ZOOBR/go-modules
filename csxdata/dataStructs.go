package csxdata

import "github.com/sirupsen/logrus"

//DataImport
type DataImport struct {
	Action string `json:"action"`
	Item   ModelActionsRunner
}

//ModelActionRunner
type ModelActionsRunner interface {
	ActionInsert(*Context)
	ActionDelete(*Context)
	ActionUpdate(*Context)
}

// ApplyActions
func ApplyActions(data []DataImport, ctx *Context) {
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
