package csxjson

import "github.com/buger/jsonparser"

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
