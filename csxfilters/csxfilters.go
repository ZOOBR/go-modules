package csxfilters

import (
	"reflect"
	"strconv"
	"strings"
	"time"

	tmt "gitlab.com/battler/modules/telemetry"
)

const (
	TypeEqual      = 1
	TypeIn         = 2
	TypeText       = 3
	TypeGte        = 4
	TypeLte        = 5
	TypeGt         = 6
	TypeLt         = 7
	TypeDateGte    = 8
	TypeDateLte    = 9
	TypeDateGt     = 10
	TypeDateLt     = 11
	TypeDatePeriod = 12
	TypeDateEqual  = 13
	TypeGroupAnd   = 14
	TypeGroupOr    = 15
	// not
	TypeNotEqual = -1
	TypeNotIn    = -2
	TypeNotText  = -3
)

// Filter base filter struct
type Filter struct {
	Field string
	Type  int
	Value string
	Items []Filter
}

// Apply function for apply filters for map[string]interface
func Apply(filters []Filter, fields map[string]interface{}) bool {
	lenFilters := len(filters)
	for i := 0; i < lenFilters; i++ {
		filter := filters[i]
		if !filter.Apply(fields) {
			return false
		}
	}
	return true
}

// ApplyGroups function for apply groups filter
func (filter *Filter) ApplyGroups(fields map[string]interface{}) bool {
	switch filter.Type {
	case TypeGroupAnd:
		for _, f := range filter.Items {
			if !f.Apply(fields) {
				return false
			}
		}
		return true
	case TypeGroupOr:
		for _, f := range filter.Items {
			if f.Apply(fields) {
				return true
			}
		}
		return false
	}
	return false
}

// Apply function for apply one filter
func (filter *Filter) Apply(fields map[string]interface{}) bool {
	// TODO:: filtering
	if filter.Items != nil && len(filter.Items) > 0 {
		return filter.ApplyGroups(fields)
	}
	var destValueInt interface{}
	var ok bool
	if strings.HasPrefix(filter.Field, "position.") {
		var err error
		destValueInt, err = tmt.GetValueFromPosition(fields, filter.Field)
		if err != nil {
			return false
		}
	} else {
		destValueInt, ok = fields[filter.Field]
		if !ok {
			return false
		}
	}
	compValInt := getFilterValueInterface(filter)
	switch filter.Type {
	case TypeEqual:
		return compareValues(compValInt, destValueInt, TypeEqual)
	case TypeNotEqual:
		return compareValues(compValInt, destValueInt, TypeNotEqual)
	case TypeText:
		return compareValues(compValInt, destValueInt, TypeText)
	case TypeNotText:
		return compareValues(compValInt, destValueInt, TypeNotText)
	case TypeGte:
		return compareValues(compValInt, destValueInt, TypeGte)
	case TypeLte:
		return compareValues(compValInt, destValueInt, TypeLte)
	case TypeGt:
		return compareValues(compValInt, destValueInt, TypeGt)
	case TypeLt:
		return compareValues(compValInt, destValueInt, TypeLt)
	case TypeDateEqual:
		return compareValues(compValInt, destValueInt, TypeDateEqual)
	case TypeDateGte:
		return compareValues(compValInt, destValueInt, TypeDateGte)
	case TypeDateLte:
		return compareValues(compValInt, destValueInt, TypeDateLte)
	case TypeDateGt:
		return compareValues(compValInt, destValueInt, TypeDateGt)
	case TypeDateLt:
		return compareValues(compValInt, destValueInt, TypeDateLt)
	case TypeIn:
		destValue, ok := destValueInt.(*string)
		if !ok || destValue == nil {
			return false
		}
		compArray := strings.Split(filter.Value, ",")
		isFound := false
		for _, item := range compArray {
			if item == *destValue {
				return true
			}
		}
		return isFound
	case TypeNotIn:
		destValue, ok := destValueInt.(*string)
		if !ok || destValue == nil {
			return false
		}
		compArray := strings.Split(filter.Value, ",")
		isFound := true
		for _, item := range compArray {
			if item == *destValue {
				return false
			}
		}
		return isFound
	}
	return true
}

func getFilter(filterItem string) *Filter {
	fieldItems := strings.Split(filterItem, "~")
	if len(fieldItems) < 3 {
		return nil
	}
	fieldName := fieldItems[0]
	fieldType, err := strconv.Atoi(fieldItems[1])
	if err != nil {
		return nil
	}
	fieldValue := fieldItems[2]
	filter := Filter{Field: fieldName, Type: fieldType, Value: fieldValue}
	return &filter
}

// FromReq return filters array from request string
// Sample use for logic or group $or->name~1~123,456->name~2~789,543
// Sample usage with simple field list $name~1~123,456$regNumber~2~789,543
func FromReq(req string) (res []Filter) {
	filters := strings.Split(req, "$")
	for _, filter := range filters {
		if filter == "" {
			continue
		}
		filterItems := strings.Split(filter, "->")
		lenFilterItems := len(filterItems)
		filterGroup := Filter{}
		if lenFilterItems < 1 {
			continue
		} else if lenFilterItems > 1 {
			groupType := filterItems[0]
			filterGroup.Items = []Filter{}
			if groupType == "or" {
				filterGroup.Type = TypeGroupOr
			} else if groupType == "and" {
				filterGroup.Type = TypeGroupAnd
			}
			for i := 1; i < len(filterItems); i++ {
				filter := getFilter(filterItems[i])
				if filter == nil {
					continue
				}
				filterGroup.Items = append(filterGroup.Items, *filter)
			}
		} else {
			filter := getFilter(filterItems[0])
			if filter == nil {
				continue
			}
			filterGroup = *filter
		}
		res = append(res, filterGroup)
	}
	return res
}

func compareValues(compVal, destVal interface{}, filterType int) bool {
	compField := reflect.ValueOf(compVal)
	if compField.Kind() == reflect.Ptr {
		compField = reflect.Indirect(compField)
	}
	destField := reflect.ValueOf(destVal)
	if destField.Kind() == reflect.Ptr {
		destField = reflect.Indirect(destField)
	}
	var comp interface{}
	var dest interface{}
	switch val := compField.Interface().(type) {
	case int, int8, int16, int32, int64:
		comp = float64(compField.Int())
	case uint, uint8, uint16, uint32, uint64:
		comp = float64(compField.Uint())
	case float32, float64:
		comp = compField.Float()
	case string:
		comp = compField.String()
	case bool:
		comp = compField.Bool()
	case time.Time:
		t, err := time.Parse(time.RFC3339Nano, val.Format(time.RFC3339Nano))
		if err != nil {
			return false
		}
		comp = t
	}
	if !destField.IsValid() {
		return false
	}
	switch val := destField.Interface().(type) {
	case int, int8, int16, int32, int64:
		dest = float64(destField.Int())
	case uint, uint8, uint16, uint32, uint64:
		dest = float64(destField.Uint())
	case float32, float64:
		dest = destField.Float()
	case string:
		dest = destField.String()
	case bool:
		dest = destField.Bool()
	case time.Time:
		t, err := time.Parse(time.RFC3339Nano, val.Format(time.RFC3339Nano))
		if err != nil {
			return false
		}
		dest = t
	}

	switch filterType {
	case TypeEqual:
		return comp == dest
	case TypeNotEqual:
		return comp != dest
	case TypeText:
		return strings.Contains(dest.(string), comp.(string))
	case TypeNotText:
		return !strings.Contains(dest.(string), comp.(string))
	case TypeGte:
		return dest.(float64) >= comp.(float64)
	case TypeLte:
		return dest.(float64) <= comp.(float64)
	case TypeGt:
		return dest.(float64) > comp.(float64)
	case TypeLt:
		return dest.(float64) < comp.(float64)
	case TypeDateEqual:
		return comp.(time.Time).Unix() == dest.(time.Time).Unix()
	case TypeDateGte:
		compTime := comp.(time.Time)
		destTime := dest.(time.Time)
		return destTime.After(compTime) || (compTime.Unix() == destTime.Unix())
	case TypeDateLte:
		compTime := comp.(time.Time)
		destTime := dest.(time.Time)
		return destTime.Before(compTime) || (compTime.Unix() == destTime.Unix())
	case TypeDateGt:
		return comp.(time.Time).After(dest.(time.Time))
	case TypeDateLt:
		return comp.(time.Time).Before(dest.(time.Time))
	}
	return false
}

func getFilterValueInterface(filter *Filter) interface{} {
	var compValInt interface{} = filter.Value
	if compValFloat, err := strconv.ParseFloat(filter.Value, 64); err == nil && filter.Type != TypeText {
		compValInt = compValFloat
	} else if compValDate, err := time.Parse("2006-01-02", filter.Value); err == nil {
		compValInt = compValDate
	} else if compValDateTime, err := time.Parse("2006-01-02 15:04:05", filter.Value); err == nil {
		compValInt = compValDateTime
	}
	return compValInt
}
