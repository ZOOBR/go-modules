package csxfilters

import (
	"strconv"
	"strings"
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

// Apply function for apply one filter
func (filter *Filter) Apply(fields map[string]interface{}) bool {
	// TODO:: filtering
	filterValueInt, ok := fields[filter.Field]
	if !ok {
		return false
	}
	filterValue, ok := filterValueInt.(*string)
	if !ok {
		return false
	}
	switch filter.Type {
	case TypeIn:
		return filterValue != nil && strings.Contains(filter.Value, *filterValue)
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
