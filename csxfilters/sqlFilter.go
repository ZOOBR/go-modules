package csxfilters

import "strings"

func genSQLFilter(field, value string, filterType int) string {
	switch filterType {
	case TypeEqual:
		return field + " = " + value
	case TypeIn:
		return field + " IN (" + value + ")"
	case TypeText:
		return field + " ILIKE '%" + value + "%'"
	case TypeGte:
		return field + " >= " + value
	case TypeLte:
		return field + " <= " + value
	case TypeGt:
		return field + " > " + value
	case TypeLt:
		return field + " < " + value
	case TypeDateGte:
		return field + " >= '" + value + "'"
	case TypeDateLte:
		return field + " <= '" + value + "'"
	case TypeDateGt:
		return field + " > '" + value + "'"
	case TypeDateLt:
		return field + " < '" + value + "'"
	// case TypeDatePeriod: // BETWEEN
	case TypeDateEqual:
		return field + " = '" + value + "'"
	case TypeNotEqual:
		return field + " <> " + value
	case TypeNotIn:
		return field + " NOT IN (" + value + ")"
	case TypeNotText:
		return field + " NOT ILIKE '%" + value + "%'"
	default:
		return ""
	}
}

func genSQLFilterGroup(filterGroup []Filter, groupType int) string {
	group := []string{}
	for i := 0; i < len(filterGroup); i++ {
		f := filterGroup[i]
		sqlFilter := prepareSQLFilter(f)
		if sqlFilter != "" {
			group = append(group, sqlFilter)
		}
	}

	switch groupType {
	case TypeGroupAnd:
		return "(" + strings.Join(group, " AND ") + ")"
	case TypeGroupOr:
		return "(" + strings.Join(group, " OR ") + ")"
	default:
		return ""
	}
}

func prepareSQLFilter(f Filter) string {
	switch f.Type {
	case TypeEqual:
		return genSQLFilter(f.Field, f.Value, TypeEqual)
	case TypeIn:
		return genSQLFilter(f.Field, f.Value, TypeIn)
	case TypeText:
		return genSQLFilter(f.Field, f.Value, TypeText)
	case TypeGte:
		return genSQLFilter(f.Field, f.Value, TypeGte)
	case TypeLte:
		return genSQLFilter(f.Field, f.Value, TypeLte)
	case TypeGt:
		return genSQLFilter(f.Field, f.Value, TypeGt)
	case TypeLt:
		return genSQLFilter(f.Field, f.Value, TypeLt)
	case TypeDateGte:
		return genSQLFilter(f.Field, f.Value, TypeDateGte)
	case TypeDateLte:
		return genSQLFilter(f.Field, f.Value, TypeDateLte)
	case TypeDateGt:
		return genSQLFilter(f.Field, f.Value, TypeDateGt)
	case TypeDateLt:
		return genSQLFilter(f.Field, f.Value, TypeDateLt)
	case TypeDatePeriod:
		return genSQLFilter(f.Field, f.Value, TypeDatePeriod)
	case TypeDateEqual:
		return genSQLFilter(f.Field, f.Value, TypeDateEqual)
	case TypeGroupAnd:
		return genSQLFilterGroup(f.Items, TypeGroupAnd)
	case TypeGroupOr:
		return genSQLFilterGroup(f.Items, TypeGroupOr)
	case TypeNotEqual:
		return genSQLFilter(f.Field, f.Value, TypeNotEqual)
	case TypeNotIn:
		return genSQLFilter(f.Field, f.Value, TypeNotIn)
	case TypeNotText:
		return genSQLFilter(f.Field, f.Value, TypeNotText)
	default:
		return ""
	}
}

func PrepareSQLFilters(filters []Filter) string {
	filterGroup := Filter{Type: TypeGroupAnd, Items: filters}
	return prepareSQLFilter(filterGroup)
}

func PrepareSQLFilterGroup(filterGroup *Filter) string {
	if filterGroup == nil || filterGroup.Items == nil {
		return ""
	}
	return prepareSQLFilter(*filterGroup)
}
