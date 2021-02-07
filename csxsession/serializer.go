package csxsession

import (
	"bytes"
	"crypto/rand"
	"encoding/base32"
	"encoding/gob"
	"encoding/json"
	"io"
	"strings"

	"github.com/buger/jsonparser"
	"github.com/gorilla/sessions"
	"gitlab.com/battler/modules/csxjson"
)

// JSONSerializer encode the session map to JSON.
type JSONSerializer struct{}

// GobSerializer uses gob package to encode the session map
type GobSerializer struct{}

// Serialize base method for full serialize
func (serializer JSONSerializer) Serialize(s *sessions.Session) (buf []byte, err error) {
	values := map[string]interface{}{}
	for k, v := range s.Values {
		var key string
		var ok bool
		if key, ok = k.(string); !ok {
			continue
		}
		values[key] = v
	}
	buf, err = json.Marshal(values)
	return buf, err
}

// Deserialize base method for full deserialize
func (serializer JSONSerializer) Deserialize(d []byte, s *sessions.Session) error {
	values := map[string]interface{}{}
	err := json.Unmarshal(d, &values)
	if err != nil {
		return err
	}
	for k, v := range values {
		s.Values[k] = v
	}
	return nil
}

// SerializePartial base method for partial serialize by key
func (serializer JSONSerializer) SerializePartial(d []byte, value []byte, s *sessions.Session, keys ...string) ([]byte, error) {
	return jsonparser.Set(d, value, keys...)
}

// DeserializePartial base method for partial deserialize by key
func (serializer JSONSerializer) DeserializePartial(d []byte, s *sessions.Session, keys ...string) error {
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

// Serialize using gob
func (s GobSerializer) Serialize(session *sessions.Session) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(session.Values)
	if err == nil {
		return buf.Bytes(), nil
	}
	return nil, err
}

// Deserialize back to map[interface{}]interface{}
func (s GobSerializer) Deserialize(d []byte, session *sessions.Session) error {
	dec := gob.NewDecoder(bytes.NewBuffer(d))
	return dec.Decode(&session.Values)
}

// SerializePartial back to map[interface{}]interface{}
func (s GobSerializer) SerializePartial(d []byte, value []byte, session *sessions.Session, keys ...string) ([]byte, error) {
	return nil, nil
}

// DeserializePartial back to map[interface{}]interface{}
func (s GobSerializer) DeserializePartial(d []byte, session *sessions.Session, keys ...string) error {
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
