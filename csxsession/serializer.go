package csxsession

import (
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"io"
	"strings"

	"github.com/buger/jsonparser"
	"github.com/gorilla/sessions"
	"gitlab.com/battler/modules/csxjson"
)

// CsxSerializer is a base struct
type CsxSerializer struct{}

// Serialize base method for full serialize
func (cs CsxSerializer) Serialize(s *sessions.Session) (buf []byte, err error) {
	buf, err = json.Marshal(s.Values)
	return buf, err
}

// Deserialize base method for full deserialize
func (cs CsxSerializer) Deserialize(d []byte, s *sessions.Session) error {
	return json.Unmarshal(d, &s.Values)
}

// SerializePartial base method for partial serialize by key
func (cs CsxSerializer) SerializePartial(d []byte, value []byte, s *sessions.Session, keys ...string) ([]byte, error) {
	return jsonparser.Set(d, value, keys...)
}

// DeserializePartial base method for partial deserialize by key
func (cs CsxSerializer) DeserializePartial(d []byte, s *sessions.Session, keys ...string) error {
	val, dataType, _, err := jsonparser.Get(d, keys...)
	if err != nil {
		return err
	}
	parsedVal, err := csxjson.GetParsedValue(val, dataType)
	if err != nil {
		return err
	}
	if len(keys) > 0 { // temporary unsupported multiple keys deserialize
		s.Values[keys[0]] = parsedVal
	}
	return nil
}

// generateRandomKey returns a new random key
func generateRandomKey() (string, error) {
	k := make([]byte, 64)
	if _, err := io.ReadFull(rand.Reader, k); err != nil {
		return "", err
	}
	return strings.TrimRight(base32.StdEncoding.EncodeToString(k), "="), nil
}
