package csxsort

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/battler/modules/telemetry"
)

// SortField struct for sort
type SortField struct {
	ID    string
	Desc  bool
	Param uint16
	Event int
}

// GetSortsFromString Converts a string to an array of structures for subsequent sorting
func GetSortsFromString(strSort *string, checkMap map[string]interface{}) (sorts []SortField) {
	if strSort == nil {
		return sorts
	}
	sorts = []SortField{}
	sortsArr := strings.Split(*strSort, ",")
	for i := 0; i < len(sortsArr); i++ {
		sortPrepare := sortsArr[i]
		parts := strings.Split(sortPrepare, ".")
		if len(parts) < 1 {
			continue
		}
		fieldName := parts[0]
		var sort SortField
		if strings.HasPrefix(sortPrepare, "position.") {
			if len(parts) < 2 {
				continue
			}
			sort = SortField{ID: fieldName}
			p, err := strconv.Atoi(parts[2])
			if err != nil {
				logrus.Warn("invalid position sort value:", sortPrepare)
				continue
			}
			if parts[1] == "p" {
				sort.Param = uint16(p)
			} else if parts[1] == "e" {
				sort.Event = p
			}
		} else {
			if checkMap != nil {
				_, ok := checkMap[fieldName]
				if !ok {
					continue
				}
			}
			sort = SortField{ID: fieldName}
			if len(parts) > 1 {
				sort.Desc = parts[1] == "1"
			}
		}
		sorts = append(sorts, sort)
	}
	return sorts
}

// SortArrayMaps Sorts an array of maps from interfaces
func SortArrayMaps(res *[]map[string]interface{}, sorts []SortField) {
	if len(sorts) == 0 {
		return
	}
	sort.SliceStable(*res, func(i, j int) bool {
		result := false
		for _, s := range sorts {
			val := (*res)[i][s.ID]
			valJ := (*res)[j][s.ID]
			if val == nil || valJ == nil {
				return val != nil
			}
			switch val.(type) {
			case string:
				str1, ok1 := val.(string)
				str2, ok2 := valJ.(string)
				if !ok1 || !ok2 {
					return false
				}
				if !s.Desc {
					return str1 < str2
				}
				return str1 > str2
			case *string:
				str1, ok1 := val.(*string)
				str2, ok2 := valJ.(*string)
				if !ok1 || !ok2 {
					return false
				}
				if str1 == nil || str2 == nil {
					return str1 != nil
				}
				if !s.Desc {
					return *str1 < *str2
				}
				return *str1 > *str2
			case *time.Time:
				time1, ok1 := val.(*time.Time)
				time2, ok2 := valJ.(*time.Time)
				if !ok1 || !ok2 {
					return false
				}
				if time1 == nil || time2 == nil {
					return time1 != nil
				}
				if !s.Desc {
					return time1.Before(*time2)
				}
				return time1.After(*time2)
			case time.Time:
				time1, ok1 := val.(time.Time)
				time2, ok2 := valJ.(time.Time)
				if !ok1 || !ok2 {
					return false
				}
				if !s.Desc {
					return time1.Before(time2)
				}
				return time1.After(time2)
			case int:
				int1, ok1 := val.(int)
				int2, ok2 := valJ.(int)
				if !ok1 || !ok2 {
					return false
				}
				if !s.Desc {
					return int1 < int2
				}
				return int1 > int2
			case *int:
				int1, ok1 := val.(*int)
				int2, ok2 := valJ.(*int)
				if !ok1 || !ok2 {
					return false
				}
				if int1 == nil || int2 == nil {
					return int1 != nil
				}
				if !s.Desc {
					return *int1 < *int2
				}
				return *int1 > *int2
			case float64:
				float1, ok1 := val.(float64)
				float2, ok2 := valJ.(float64)
				if !ok1 || !ok2 {
					return false
				}
				if !s.Desc {
					return float1 < float2
				}
				return float1 > float2
			case *float64:
				float1, ok1 := val.(*float64)
				float2, ok2 := valJ.(*float64)
				if !ok1 || !ok2 {
					return false
				}
				if float1 == nil || float2 == nil {
					return float1 != nil
				}
				if !s.Desc {
					return *float1 < *float2
				}
				return *float1 > *float2
			case *telemetry.FlatPosition:
				if s.Param == 0 && s.Event == 0 {
					return false
				}
				pos1, ok1 := val.(*telemetry.FlatPosition)
				pos2, ok2 := valJ.(*telemetry.FlatPosition)
				if !ok1 || !ok2 {
					return false
				}
				if pos1 == nil || pos2 == nil {
					return pos1 != nil
				}
				if s.Param != 0 {
					p1 := pos1.P[s.Param]
					p2 := pos2.P[s.Param]
					if !s.Desc {
						return p1 < p2
					}
					return p1 > p2
				}
				p1 := pos1.E[s.Param]
				p2 := pos2.E[s.Param]
				if !s.Desc {
					return p1 < p2
				}
				return p1 > p2

			default:
				return false
			}
		}
		return result
	})
}
