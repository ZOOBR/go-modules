package strings

import (
	"math/rand"

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
