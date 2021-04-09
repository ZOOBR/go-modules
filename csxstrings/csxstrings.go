package csxstrings

import (
	"math/rand"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

func RandomString(n int, onlyDigits bool) string {
	var letter []rune
	if onlyDigits {
		letter = []rune("0123456789")
	} else {
		letter = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	}

	b := make([]rune, n)
	for i := range b {
		b[i] = letter[rand.Intn(len(letter))]
	}
	return string(b)
}

func IsValidUUID(u string) bool {
	_, err := uuid.Parse(u)
	return err == nil
}

func NewId() (res *string) {
	uuidVal := uuid.New().String()
	return &uuidVal
}

func NewUUID() (res string) {
	uuidVal := uuid.New().String()
	return uuidVal
}

func GetDateFormat(format string) (string, bool) {
	newFormat := "20060102"
	if len(format) > 8 {
		newFormat += "15"
	}
	if len(format) > 10 {
		newFormat += "04"
	}
	if len(format) > 12 {
		newFormat += "05"
	}
	return newFormat, len(format) > 8
}

func GetIdsStr(ids string) string {
	idsArray := strings.Split(ids, ",")
	for key, _ := range idsArray {
		idsArray[key] = "'" + idsArray[key] + "'"
	}
	return strings.Join(idsArray, ",")
}

// GetStructFields extract all json fields from structure
func GetStructFields(s interface{}) []string {
	rt := reflect.TypeOf(s)
	out := []string{}
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag == "-" {
			continue
		}
		jsonKeys := strings.Split(jsonTag, ",")
		if len(jsonKeys) == 0 {
			continue
		}
		jsonKey := jsonKeys[0]
		if jsonKey != "" {
			out = append(out, jsonKey)
		}
	}
	return out
}

func SplitPascal(str string, delimiter string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}"+delimiter+"${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}"+delimiter+"${2}")
	return snake
}

func ParseTimeString(str *string, timeFormat *string) (*time.Time, error) {
	var result *time.Time
	if str != nil && *str != "" && *str != "0" {
		if timeFormat != nil && *timeFormat == "ms" {
			msInt, err := strconv.ParseInt(*str, 10, 64)
			if err != nil {
				return nil, err
			}
			parsedTime := time.Unix(0, msInt*int64(time.Millisecond)).UTC()
			return &parsedTime, nil
		}
		f, _ := GetDateFormat(*str)
		t, err := time.Parse(f, *str)
		if err != nil {
			return nil, err
		}
		tu := t.UTC()
		result = &tu
	}
	return result, nil
}

func GetStrMapFromList(list *string) map[string]bool {
	result := make(map[string]bool)
	if list == nil || *list == "" {
		return result
	}
	if list != nil {
		arr := strings.Split(*list, ",")
		for _, val := range arr {
			val = strings.TrimSpace(val)
			result[val] = true
		}
	}
	return result
}

func GetIntMapFromList(list *string, zero bool) map[int]bool {
	result := make(map[int]bool)
	if list != nil {
		arr := strings.Split(*list, ",")
		for i := 0; i < len(arr); i++ {
			str := arr[i]
			if len(str) > 0 {
				v, _ := strconv.Atoi(str)
				if v != 0 || zero {
					result[v] = true
				}
			}
		}
	}
	return result
}
