package csxjson

import (
	"errors"
	"reflect"

	"github.com/buger/jsonparser"
)

func isPointer(data interface{}) bool {
	return reflect.ValueOf(data).Type().Kind() == reflect.Ptr
}

// Unmarshal unmarshal parses the JSON-encoded data and stores the result in the value pointed to by v
func Unmarshal(data []byte, value interface{}) (err error) {
	if !isPointer(value) {
		return errors.New("Unmarshal work only with pointer data")
	}
	val, dataType, _, err := jsonparser.Get(data)
	if err != nil {
		return err
	}
	parsedVal, errParse := GetParsedValue(val, dataType)
	if errParse != nil {
		return errParse
	}
	switch dataType {
	case jsonparser.Number:
		v := value.(*float64)
		*v = parsedVal.(float64)
	case jsonparser.String:
		v := value.(*string)
		*v = parsedVal.(string)
	case jsonparser.Array:
		v := value.(*[]interface{})
		*v = parsedVal.([]interface{})
	case jsonparser.Object:
		v := value.(*map[string]interface{})
		*v = parsedVal.(map[string]interface{})
	default:
		value = nil
	}
	return nil
}

// GetParsedValue parse recursively byte array
func GetParsedValue(data []byte, dataType jsonparser.ValueType) (parsedVal interface{}, err error) {
	switch dataType {
	case jsonparser.NotExist:
		parsedVal = data
	case jsonparser.String:
		stringValue, err := jsonparser.ParseString(data)
		if err == nil {
			parsedVal = stringValue
		}
	case jsonparser.Number:
		numberValue, err := jsonparser.ParseFloat(data)
		if err == nil {
			parsedVal = numberValue
		}
	case jsonparser.Object:
		objectValue := map[string]interface{}{}
		jsonparser.ObjectEach(data, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
			keyString := string(key)
			pVal, err := GetParsedValue(value, dataType)
			if err == nil {
				objectValue[keyString] = pVal
			}
			return err
		})
		parsedVal = objectValue
	case jsonparser.Array:
		arrayValue := []interface{}{}
		jsonparser.ArrayEach(data, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			pVal, err := GetParsedValue(value, dataType)
			if err == nil {
				arrayValue = append(arrayValue, pVal)
			}
		})
		parsedVal = arrayValue
	case jsonparser.Boolean:
		boolValue, err := jsonparser.ParseBoolean(data)
		if err == nil {
			parsedVal = boolValue
		}
	case jsonparser.Null:
	default:
		parsedVal = data

	}
	return parsedVal, err
}
