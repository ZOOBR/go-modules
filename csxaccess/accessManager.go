package csxaccess

import (
	"math"
	"regexp"
	"sort"
	"sync"

	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
	dbc "gitlab.com/battler/modules/sql"
)

const (
	// QueryModeRead mode for read collections items
	QueryModeRead = 1
	// QueryModeCreate mode for upsert and update
	QueryModeCreate = 2
	// QueryModeUpdate mode for upsert and update
	QueryModeUpdate = 4
	// QueryModeDelete mode for delet collection items
	QueryModeDelete = 8
)

type AccessManager struct {
	accessMap   sync.Map
	categoryMap sync.Map
	category    func(mandat *Mandat) (string, string)
}

// Mandat is a base partner struct
type Mandat struct {
	ID       string
	Subject  string
	Group    string
	Role     *string
	Fields   []string
	Priority int
	Access   int
	Params   map[string]interface{}
}

// CheckAccess check manadat access
func (mandat *Mandat) CheckAccess(roles map[string]interface{}) int {
	if mandat.CheckRole(roles) {
		return mandat.Access
	}
	return 0
}

// Assign assign mandat properties
func (mandat *Mandat) Assign(extMandat *Mandat) *Mandat {
	if extMandat.Access > 0 {
		mandat.Access &= extMandat.Access
	} else if extMandat.Access < 0 {
		mandat.Access ^= extMandat.Access * -1
	}

	if extMandat.Fields != nil && mandat.Fields != nil {
		for i := 0; i < len(extMandat.Fields); i++ {
			extField := (extMandat.Fields)[i]
			isFind := false
			for j := 0; j < len(mandat.Fields); j++ {
				field := (mandat.Fields)[j]
				if field == extField {
					isFind = true
					break
				}
			}
			if !isFind {
				mandat.Fields = append(mandat.Fields, extField)
			}
		}
	} else if extMandat.Fields != nil && mandat.Fields == nil {
		mandat.Fields = extMandat.Fields
	}
	return mandat
}

// CheckMandatParams checks mandat params permission
func (mandat *Mandat) CheckMandatParams(urlParams map[string]string) bool {
	if mandat.Params == nil {
		return true
	}
	result := true
	for paramName, paramValueInt := range mandat.Params {
		paramValue, ok := paramValueInt.(string)
		if !ok {
			continue
		}

		if paramValue != urlParams[paramName] {
			result = false
			break
		}

		if !result {
			break
		}

	}
	return result
}

// CheckRole check manadat role
func (mandat *Mandat) CheckRole(roles map[string]interface{}) bool {
	_, ok := roles[*mandat.Role]
	return ok
}

func createNewFields(fields []string, newFields []string, strictFields map[string]bool, db bool) {
	idx := 0
	for _, field := range fields {
		if strictFields != nil {
			if _, ok := strictFields[field]; !ok {
				continue
			}
		}
		if db {
			// check alphanumeric
			pattern := `^[a-zA-Z0-9]*$`
			matched, _ := regexp.Match(pattern, []byte(field))
			if matched {
				newFields[idx] = `"` + field + `"`
			} else {
				newFields[idx] = field
			}
		} else {
			newFields[idx] = field
		}

		idx++
	}
}

// StrictAccess check user access to clollection with read, update and delete modes
func (manager *AccessManager) StrictAccess(subject string, mode int, fields []string, roles map[string]interface{}, db bool, isSuperUser bool) (bool, []string) {
	success := false
	if isSuperUser {
		newFields := make([]string, len(fields))
		createNewFields(fields, newFields, nil, db)
		return true, newFields
	}
	mandatsInt, ok := manager.accessMap.Load(subject)
	if !ok {
		return success, nil
	}
	mandats, ok := mandatsInt.([]*Mandat)
	if !ok {
		return success, nil
	}
	countMandats := len(mandats)
	strictFields := make(map[string]bool)
	for i := 0; i < countMandats; i++ {
		mandat := mandats[i]
		access := mandat.CheckAccess(roles)

		// prohibit access to fields if prohibited
		denyMode := access <= 0
		if denyMode && mandat.Fields == nil {
			continue
		}

		// mandates less than zero prohibit access
		currentAccess := int(math.Abs(float64(access)))
		if mode&currentAccess == 0 {
			continue
		}

		// There may be several mandates, they are applied in accordance with priority
		// As a result, the decision to ban or allow the operation will be behind the mandate with the highest priority
		success = true

		// If no access fields for the credential are specified, then we consider that all fields are available
		if mandat.Fields == nil {
			// set all fields
			for _, field := range fields {
				strictFields[field] = true
			}
			continue
		}

		// All fields requested for work are bypassed and fields are checked according to the mandate.
		// Deny credentials remove fields that allow add
		lenFields := len(fields)
		for i := 0; i < lenFields; i++ {
			field := (fields)[i]
			for _, mandatField := range mandat.Fields {
				// * allows all fields
				if mandatField == "*" {
					if denyMode {
						strictFields = make(map[string]bool)
						break
					}
					for _, field := range fields {
						strictFields[field] = true
					}
				}
				if mandatField == field && denyMode {
					delete(strictFields, field)
				} else if mandatField == field {
					strictFields[field] = true
				}
			}
		}
	}
	var lenFields int
	if isSuperUser {
		lenFields = len(fields)
	} else {
		lenFields = len(strictFields)
	}
	newFields := make([]string, lenFields)
	createNewFields(fields, newFields, strictFields, db)
	return success, newFields
}

// Load ---
func (manager *AccessManager) Load(mandats []*Mandat, print bool) {
	for _, mandatIn := range mandats {
		mandat := mandatIn
		var mandats []*Mandat
		category, subject := manager.category(mandat)
		if subject != "" {
			mandat.Subject = subject
		}
		mandatsInt, ok := manager.accessMap.Load(mandat.Subject)
		if ok {
			mandats = mandatsInt.([]*Mandat)
			mandats = append(mandats, mandat)
		} else {
			mandats = []*Mandat{mandat}
		}
		manager.accessMap.Store(mandat.Subject, mandats)
		if category != "" {
			manager.categoryMap.Store(category, mandats)
		}
	}

	if print {
		manager.accessMap.Range(func(key, val interface{}) bool {
			logrus.Debug("load mandat:", key)
			return true
		})
		manager.categoryMap.Range(func(key, val interface{}) bool {
			logrus.Debug("load mandat category:", key)
			return true
		})
	}
}

func sortMandats(mandatsNew []*Mandat) (result []*Mandat) {
	sorts := []string{"role", "group", "priority"}
	for i := 0; i < len(sorts); i++ {
		sortID := sorts[i]
		sort.SliceStable(mandatsNew, func(i, j int) bool {
			elem1 := mandatsNew[i]
			elem2 := mandatsNew[j]

			if sortID == "role" {
				if elem1.Role == nil || elem2.Role == nil {
					return elem1.Role != nil
				}
				return *elem1.Role < *elem2.Role
			} else if sortID == "group" {
				if elem1.Group == "" || elem2.Group == "" {
					return elem1.Group != ""
				}
				return elem1.Group < elem2.Group
			}
			return elem1.Priority < elem2.Priority
		})
	}
	return mandatsNew
}

// GetCategoryMandats ---
func (manager *AccessManager) GetCategoryMandats(category string) ([]*Mandat, bool) {
	mandatsInt, ok := manager.categoryMap.Load(category)
	return mandatsInt.([]*Mandat), ok
}

// GetMandatsBySubject ---
func (manager *AccessManager) GetMandatsBySubject(subject string, roles map[string]interface{}) ([]*Mandat, bool) {
	mandatsInt, ok := manager.accessMap.Load(subject)
	if !ok {
		return nil, false
	}
	mandats := mandatsInt.([]*Mandat)
	if mandats == nil {
		return nil, true
	}
	roleMandats := []*Mandat{}
	mandat := &Mandat{}
	for i := 0; i < len(mandats); i++ {
		currentMandat := mandats[i]
		if mandat.ID == "" {
			mandat = currentMandat
		}
		if !currentMandat.CheckRole(roles) {
			continue
		}
		if mandat.ID != currentMandat.ID {
			mandat.Assign(currentMandat)
		}
		roleMandats = append(roleMandats, mandat)
	}
	return roleMandats, true
}

// CheckNeedRedirect check need redirect to another app server
func CheckNeedRedirect(ctx echo.Context, clientAppServer string, id string) (bool, string) {
	request := ctx.Request()
	if len(clientAppServer) == 0 {
		return false, ""
	}
	var scheme string
	if request.TLS == nil {
		scheme = "http"
	} else {
		scheme = "https"
	}
	appServerURL := scheme + "://" + request.Host
	if clientAppServer != "" && clientAppServer != appServerURL {
		if appServerURL != "" {
			redirectTo := clientAppServer + request.URL.Path + "?" + request.URL.RawQuery
			logrus.WithFields(map[string]interface{}{"route": 123, "from:": appServerURL, "to:": redirectTo}).Info("client " + id + " redirected")
			return true, redirectTo
		}
		logrus.Error("App server URL is missing! Redirection of client " + id + " to " + clientAppServer + " is not performed")
	}
	return false, ""
}

// GetRolesRights returns map of role interfaces from string
func GetRolesRights(roles map[string]interface{}) map[string]*dbc.JsonB {
	result := map[string]*dbc.JsonB{}
	for roleID, roleInt := range roles {
		role := roleInt.(map[string]interface{})
		rightsInt := role["rights"]
		if rightsInt == nil {
			continue
		}
		rights := dbc.JsonB(rightsInt.(map[string]interface{}))
		result[roleID] = &rights
	}
	return result
}
