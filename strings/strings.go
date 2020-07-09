package strings

import (
	"math/rand"
	"reflect"
	"strings"

	"github.com/google/uuid"
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
		jsonKeys := strings.Split(field.Tag.Get("json"), ",")
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
