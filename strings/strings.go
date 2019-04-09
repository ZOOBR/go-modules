package strings

import (
	"math/rand"
	"strings"

	"github.com/prometheus/common/log"
	uuid "github.com/satori/go.uuid"
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
	_, err := uuid.FromString(u)
	return err == nil
}

func NewId() (res *string) {
	u2, err := uuid.NewV4()
	if err != nil {
		log.Error("error generate uuid:", err)
		return res
	}
	uuidVal := u2.String()
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
