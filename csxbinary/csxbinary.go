package csxbinary

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	//binary protocol constants
	protocolVersion1   = 0
	protocolVersion2   = 1 // v2
	protocolTypeBinary = 0
	protocolTypeJSON   = 1
	baseSign           = 16
	binaryEmpty        = 0x00
	binaryZero         = 0x10
	binaryInt8         = 0x01
	binaryUint8        = 0x11
	binaryInt16        = 0x02
	binaryUint16       = 0x12
	binaryInt32        = 0x04
	binaryUint32       = 0x14
	binaryFloat32      = 0x24
	binaryFloat64      = 0x28
	binaryArrayEvents  = 0x40
	binaryArray        = 0x41
	binaryString       = 0x53 // v2
	paramEvent         = 2
	// TODO: add Array type and rename current array to events
	//struct type
	structInt16     = 0
	structUint32    = 1
	structF32       = 2
	errorBinarySize = 303
	errorBinaryLen  = 304
	errorProtoTime  = 305
	errorBinaryRead = 306
)

var mapParamsPrecision = map[uint16]float64{
	1021: 100,
	1022: 100,
	1101: 1000000000,
	1102: 1000000000,
	1201: 1000,
}

var (
	binaryID = []rune("bt")
)

// PositionInfo - position info (base check content)
type PositionInfo struct {
	Time float64       `json:"time"`
	ID   string        `json:"id"`
	Type string        `json:"type"`
	Data *FlatPosition `json:"data"`
}

// ParamsFloat32 some type
type ParamsFloat32 struct {
	K uint16
	V float32
}

type ParamsFloat64 struct {
	K uint16
	V float64
}

type ParamsInt16 struct {
	K uint16
	V int16
}

type ParamsUint16 struct {
	K uint16
	V int16
}

type ParamsUint32 struct {
	K uint16
	V uint32
}

// ZoneInfo zone information
type ZoneInfo struct {
	ID       string   `json:"id"`
	Name     string   `json:"name"`
	Path     string   `json:"path"`
	Type     string   `json:"tid"`
	Additive *float64 `json:"additive,omitempty"`
	Distance *float64 `json:"distance,omitempty"`
}

// FlatPosition compact position format
type FlatPosition struct {
	Time  float64            `json:"t"`
	P     map[uint16]float64 `json:"p,omitempty"`
	E     []uint16           `json:"e,omitempty"`
	Zones []ZoneInfo         `json:"zones,omitempty"`
}

// BinaryReader base struct for read binary data
type BinaryReader struct {
	Buf       []byte
	offset    uint32
	Size      uint32
	Params    map[uint16]bool // old
	Events    map[uint16]bool // old
	BeginTime time.Time
	EndTime   time.Time
	lenEvents int
	lenParams int
	lenElems  int                    // for new protocol
	flatPos   *FlatPosition          // old
	Data      map[string]interface{} //for new protocol
	pass      bool
}

// BinaryData for read binaryPosition
type BinaryData struct {
	Day  string
	Time float64
	Data []byte
}

// param for map
type param struct {
	P uint16 // param code
	T uint16 // linked time code or zero
	H bool   // need load history if parameter not exists
}

// Params map with telemetry params
var Params = map[string]param{
	"paramTimeCan":        {8, 0, false},
	"paramTimeReceive":    {9, 0, false},
	"paramTimeFuel":       {10, 0, false},
	"paramTimeFuel2":      {11, 0, false},
	"paramStatus":         {1020, 0, false},
	"paramBatteryV":       {1022, 0, false},
	"paramAngle":          {1104, 0, false},
	"paramSpeed":          {1105, 0, false},
	"paramLat":            {1101, 0, true},
	"paramLon":            {1102, 0, true},
	"paramOdometer":       {1201, 0, true},
	"paramMileageReserve": {1203, 0, false},
	"paramCanStatus":      {2200, 0, false},
	"paramCanTempr":       {2201, 0, false},
	"paramCanSpeed":       {2202, 0, false},
	"paramAdc0":           {2000, 0, false},
	"paramAdc1":           {2001, 0, false},

	"paramCalcOdo":    {3001, 0, false},
	"paramDriftLevel": {3004, 0, false},
	"paramSpeedAvg":   {3005, 0, false},
	"paramFuel":       {3400, 0, true},
	"paramFuel2":      {3401, 0, false},
	"paramAvgFuel":    {3500, 0, true},
	"paramAvgFuel2":   {3501, 0, false},
}

// GetParamCode prepare uit16 code from interface
func GetParamCode(id interface{}) uint16 {
	switch id.(type) {
	case int:
		return uint16(id.(int))
	case uint:
		return uint16(id.(uint))
	case uint16:
		return uint16(id.(uint16))
	case float64:
		return uint16(id.(float64))
	case string:
		sid := id.(string)
		if val, err := strconv.Atoi(sid); err == nil {
			return uint16(val)
		} else if p, ok := Params[sid]; ok {
			return p.P
		}
	}
	return 0
}

// Get position param value by name
func (pos *FlatPosition) Get(id interface{}) (float64, bool) {
	code := GetParamCode(id)
	if code > 0 {
		val, ok := pos.P[code]
		return val, ok
	}
	return 0, false
}

// GetVal position param value by name
func (pos *FlatPosition) GetVal(id interface{}) float64 {
	val, _ := pos.Get(id)
	return val
}

// Set position param value by name
func (pos *FlatPosition) Set(id interface{}, val float64, index ...int) {
	code := GetParamCode(id)
	if len(index) > 0 {
		code += uint16(index[0])
	}
	pos.P[code] = val
}

// IfEvent checks for an event in the list
func (pos *FlatPosition) IfEvent(event uint16) bool {
	if pos.E == nil {
		return false
	}
	for i := 0; i < len(pos.E); i++ {
		if pos.E[i] == event {
			return true
		}
	}
	return false
}

// ParamCode get param code by name
func ParamCode(name string) uint16 {
	return Params[name].P
}

// FuelParams array with fuel params
var FuelParams = []uint16{
	ParamCode("paramFuel"),
	ParamCode("paramFuel2"),
}

// FuelAvgParams array with avg fuel params
var FuelAvgParams = []uint16{
	ParamCode("paramAvgFuel"),
	ParamCode("paramAvgFuel2"),
}

// FindZone is search zone by id
func FindZone(list []ZoneInfo, id string) *ZoneInfo {
	cnt := len(list)
	for i := 0; i < cnt; i++ {
		zone := list[i]
		if zone.ID == id {
			return &zone
		}
	}
	return nil
}

func ReadUint64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}

func ReadUint32(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf)
}

func ReadUint16(buf []byte) uint16 {
	return binary.BigEndian.Uint16(buf)
}

func ReadFloat32(buf []byte) float32 {
	return math.Float32frombits(binary.BigEndian.Uint32(buf))
}

func ReadFloat64(buf []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(buf))
}

// CheckSign check sign
func (r *BinaryReader) CheckSign() bool {
	if r.Buf[r.offset] != uint8(binaryID[0]) || (r.Buf)[r.offset+1] != uint8(binaryID[1]) {
		return false
	}
	return true
}

// CheckVersion check protocol version (0: invalid)
func (r *BinaryReader) CheckVersion() int {
	offset := 2
	switch v := ReadUint16(r.Buf[offset : offset+2]); v {
	case protocolVersion1:
		return 1
	case protocolVersion2:
		return 2
	default:
		return 0
	}
}

// ReadArrayEvents events (old protocol)
func (r *BinaryReader) ReadArrayEvents(kind uint8) int16 {
	bufLen := uint32(len(r.Buf))
	nibble := uint32(kind & 0x0F)
	if bufLen < r.offset+2 {
		return errorBinaryLen
	}
	elCount := uint32(ReadUint16(r.Buf[r.offset : r.offset+2]))
	r.offset += 2
	if bufLen < r.offset+elCount*nibble {
		return errorBinaryLen
	}

	r.flatPos.E = make([]uint16, 0)

	for index := uint32(0); index < elCount; index++ {
		err := r.ReadFlatValue(binaryArrayEvents, uint16(index))
		if err != 0 {
			continue
		}
	}
	return 0
}

// Reset write len's to struct and reset offset
func (r *BinaryReader) Reset() {
	r.offset = 0
	r.Size = uint32(len(r.Buf))
	r.lenEvents = len(r.Events)
	r.lenParams = len(r.Params)
}

// ReadString parse and return string
func (r *BinaryReader) ReadString() string {
	len := r.ReadUint32()
	buf := r.Buf[r.offset : r.offset+len]
	r.offset += len
	return string(buf)
}

// ReadArray parse binary array
func (r *BinaryReader) ReadArray() []interface{} {
	log.Info("read array start")
	len := int(r.ReadUint32())
	var result []interface{}
	result = make([]interface{}, len)
	for i := 0; i < len; i++ {
		// read type
		itemType := r.Buf[r.offset]
		r.offset++
		// check type
		switch itemType {
		case binaryZero:
			result[i] = float64(0)
		case binaryInt8:
			result[i] = int8(r.ReadInt8())
		case binaryUint8:
			result[i] = uint8(r.ReadUint8())
		case binaryInt16:
			result[i] = int16(r.ReadInt16())
		case binaryUint16:
			result[i] = uint16(r.ReadUint16())
		case binaryInt32:
			result[i] = int32(r.ReadInt32())
		case binaryUint32:
			result[i] = uint32(r.ReadUint32())
		case binaryFloat64:
			result[i] = float64(r.ReadFloat64())
		case binaryFloat32:
			result[i] = float32(r.ReadFloat32())
		case binaryString:
			result[i] = r.ReadString()
		case binaryArray:
			result[i] = r.ReadArray()
		default:
			log.Error("ReadArray: undefined type")
		}

	}

	return result
}

// ReadInt8 read int8 and inc offset
func (r *BinaryReader) ReadInt8() int8 {
	v := int8(r.Buf[r.offset])
	r.offset++
	return v
}

func (r *BinaryReader) ReadUint8() uint8 {
	return uint8(r.ReadInt8())
}

func (r *BinaryReader) ReadInt16() int16 {
	v := int16(ReadUint16(r.Buf[r.offset : r.offset+2]))
	r.offset += 2
	return v
}

func (r *BinaryReader) ReadUint16() uint16 {
	v := ReadUint16(r.Buf[r.offset : r.offset+2])
	r.offset += 2
	return v
}

func (r *BinaryReader) ReadInt32() int32 {
	v := int32(ReadUint32(r.Buf[r.offset : r.offset+4]))
	r.offset += 4
	return v
}

func (r *BinaryReader) ReadUint32() uint32 {
	v := ReadUint32(r.Buf[r.offset : r.offset+4])
	r.offset += 4
	return v
}

func (r *BinaryReader) ReadFloat32() float32 {
	bits := binary.BigEndian.Uint32(r.Buf[r.offset : r.offset+4])
	float := math.Float32frombits(bits)
	r.offset += 4
	return float
}

func (r *BinaryReader) ReadFloat64() float64 {
	bits := binary.BigEndian.Uint64(r.Buf[r.offset : r.offset+8])
	float := math.Float64frombits(bits)
	r.offset += 8
	return float
}

func float64ToByte(f float64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(f))
	return buf[:]
}

func float32ToByte(f float32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], math.Float32bits(f))
	return buf[:]
}

func int16ToByte(f int16) []byte {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(f))
	return buf[:]
}

func int32ToByte(f int32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(f))
	return buf[:]
}

func uint16ToByte(f uint16) []byte {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(f))
	return buf[:]
}

func uint32ToByte(f uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(f))
	return buf[:]
}

func getNumberKind(value float64) int {
	if value == float64(int64(value)) {
		if value == 0 {
			return binaryZero
		}
		if value < 0 {
			if value >= -128 {
				return binaryInt8
			}
			if value >= -32768 {
				return binaryInt16
			}
			return binaryInt32
		}
		if value < 0xff {
			return binaryUint8
		}
		if value < 0xffff {
			return binaryUint16
		}
		if value < 0xffffffff {
			return binaryUint32
		}
		return binaryFloat64
	}
	if value > 10000000 {
		return binaryFloat64
	}
	return binaryFloat32
}

func writeNumber(value float64, bytes *[]byte) {

	kind := getNumberKind(value)

	var res []byte
	switch kind {
	case binaryInt8:
		res = []byte{byte(int8(value))}
	case binaryInt16:
		res = int16ToByte(int16(value))
	case binaryInt32:
		res = int32ToByte(int32(value))
	case binaryUint8:
		res = []byte{byte(uint8(value))}
	case binaryUint16:
		res = uint16ToByte(uint16(value))
	case binaryUint32:
		res = uint32ToByte(uint32(value))
	case binaryFloat32:
		res = float32ToByte(float32(value))
	case binaryFloat64:
		res = float64ToByte(value)
	}

	*bytes = append(*bytes, byte(kind))
	if len(res) > 0 {
		*bytes = append(*bytes, res...)
	}

}

func (r *BinaryReader) ReadLen() uint32 {
	v := ReadUint32(r.Buf[r.offset : r.offset+4])
	r.offset += 4
	return v
}

func (r *BinaryReader) ReadTime() float64 {
	v := ReadFloat64(r.Buf[r.offset : r.offset+8])
	return v
}

func (r *BinaryReader) ReadKey() uint16 {
	v := ReadUint16(r.Buf[r.offset : r.offset+2])
	r.offset += 2
	return v
}

func (r *BinaryReader) ReadKind() uint8 {
	v := uint8(r.Buf[r.offset])
	r.offset++
	return v
}

func (r *BinaryReader) Skip(kind uint8, key uint16) {
	switch kind {
	case binaryInt8:
		r.offset++
	case binaryUint8:
		r.offset++
	case binaryInt16:
		r.offset += 2
	case binaryUint16:
		r.offset += 2
	case binaryInt32:
		r.offset += 4
	case binaryUint32:
		r.offset += 4
	case binaryFloat32:
		r.offset += 4
	case binaryFloat64:
		r.offset += 8
	}
}

func FlatPositionToBinary(pos *FlatPosition) (res []byte) {

	// position structure
	// 2 byte sign + 2 byte version + 4 bytes length + 8 bytes time + params and events
	sign := []byte{98, 116, 0, 0}
	res = append(res, sign...)
	var data []byte
	for p := range pos.P {
		data = append(data, uint16ToByte(uint16(p))...)
		writeNumber(pos.P[p], &data)
	}
	if len(pos.E) > 0 {
		data = append(data, uint16ToByte(uint16(paramEvent))...)
		data = append(data, byte(binaryArrayEvents|binaryUint16))
		data = append(data, uint16ToByte(uint16(len(pos.E)))...)
		for e := range pos.E {
			data = append(data, uint16ToByte(uint16(pos.E[e]))...)
		}
	}

	res = append(res, uint32ToByte(uint32(len(data)+8))...)
	res = append(res, float64ToByte(pos.Time)...)
	res = append(res, data...)

	return res
}

func stringToByte(str string) []byte {
	// 1 byte type + 2 byte length + some string (utf8)
	data := []byte{binaryString}
	data = append(data, uint32ToByte(uint32(len(str)))...)
	data = append(data, []byte(str)...)
	return data
}

func arrayToByte(arr []interface{}) []byte {
	// 1 byte type + 2byte length + elements[]: type + length(optional) + value)
	data := []byte{binaryArray}
	arrLen := len(arr)
	data = append(data, uint32ToByte(uint32(arrLen))...)
	for _, v := range arr {
		switch v.(type) {
		case int:
			writeNumber(float64(v.(int)), &data)
		case int8:
			writeNumber(float64(v.(int8)), &data)
		case int16:
			writeNumber(float64(v.(int16)), &data)
		case int32:
			writeNumber(float64(v.(int32)), &data)
		case uint8:
			writeNumber(float64(v.(uint8)), &data)
		case uint16:
			writeNumber(float64(v.(uint16)), &data)
		case uint32:
			writeNumber(float64(v.(uint32)), &data)
		case float32:
			writeNumber(float64(v.(float32)), &data)
		case float64:
			writeNumber(float64(v.(float64)), &data)
		case string:
			data = append(data, stringToByte(string(v.(string)))...)
		case []interface{}:
			arrayToByte(v.([]interface{}))
		default:
			log.Error("MapToBinary(arrayToByte): undefined type")
		}
	}

	return data
}

// MapToBinary convert data to bytes with use protocol v2
func MapToBinary(inputData map[string]interface{}) (res []byte) {

	// position structure
	// 2 byte sign + 2 byte version + 4 bytes length + string key/interface{} value
	sign := []byte{98, 116, 0, 0}
	res = append(res, sign...)
	var data []byte
	for k, v := range inputData {
		// add string key
		data = append(data, stringToByte(k)...)
		// check value type
		switch v.(type) {
		case int:
			writeNumber(float64(v.(int)), &data)
		case uint:
			writeNumber(float64(v.(uint)), &data)
		case int64:
			writeNumber(float64(v.(int64)), &data)
		case float32:
			writeNumber(float64(v.(float32)), &data)
		case float64:
			writeNumber(float64(v.(float64)), &data)
		case string:
			data = append(data, stringToByte(string(v.(string)))...)
		case []interface{}:
			data = append(data, arrayToByte(v.([]interface{}))...)
		default:
			log.Error("MapToBinary: undefined type")
		}
	}
	// append data to result
	res = append(res, uint32ToByte(uint32(len(inputData)))...)
	res = append(res, data...)

	return res
}

// ReadData convert binary to map and save to BinaryReader.Data
func (r *BinaryReader) ReadData() int16 {
	r.Reset()
	// init result variable
	var result map[string]interface{}
	result = make(map[string]interface{})
	//check size
	if r.Size < 8 {
		return errorBinarySize
	}
	//check sign and pass invalid data
	if !r.CheckSign() {
		return errorBinarySize
	}
	// sign and version (2+2 bytes)
	r.offset += 4
	//read size of elements
	r.lenElems = int(r.ReadUint32())
	//parse elements (string key + any value)
	for i := 0; i < r.lenElems; i++ {
		//string: 1byte type 2byte size
		//check type
		st := r.Buf[r.offset]
		if st != binaryString {
			return errorBinaryRead
		}
		r.offset++
		// read string
		keyName := r.ReadString()
		// check value type
		vt := r.Buf[r.offset]
		r.offset++
		switch vt {
		case binaryZero:
			result[keyName] = float64(0)
		case binaryInt8:
			result[keyName] = int8(r.ReadInt8())
		case binaryUint8:
			result[keyName] = uint8(r.ReadUint8())
		case binaryInt16:
			result[keyName] = int16(r.ReadInt16())
		case binaryUint16:
			result[keyName] = uint16(r.ReadUint16())
		case binaryInt32:
			result[keyName] = int32(r.ReadInt32())
		case binaryUint32:
			result[keyName] = uint32(r.ReadUint32())
		case binaryFloat64:
			result[keyName] = float64(r.ReadFloat64())
		case binaryFloat32:
			result[keyName] = float32(r.ReadFloat32())
		case binaryString:
			result[keyName] = r.ReadString()
		case binaryArray:
			result[keyName] = r.ReadArray()
		default:
			log.Error(vt)
			return errorBinaryRead
		}
	}

	r.Data = result

	return 0
}

func (reader *BinaryReader) ReadFlatPositions() (int16, []FlatPosition) {
	reader.Reset()
	posArr := make([]FlatPosition, 0)
	for reader.Size > reader.offset {
		res := reader.readPosition()
		if res < 0 {
			reader.offset++
			continue
		}
		if res == errorBinarySize || res == errorBinaryLen || res == errorBinaryRead || res == errorProtoTime {
			return res, posArr
		}
		if reader.flatPos != nil && (reader.lenEvents == 0 || reader.pass == true) {
			posArr = append(posArr, *reader.flatPos)
		}
	}
	return 0, posArr
}

func (reader *BinaryReader) readPosition() int16 {

	// position structure
	// 2 byte sign + 2 byte version + 4 bytes length + 8 bytes time + params and events
	reader.pass = false
	//check size
	if reader.Size < reader.offset+8 {
		return errorBinarySize
	}
	//check sign and pass invalid data
	for !reader.CheckSign() {
		log.Warn("check sign err:", reader.Buf[reader.offset:reader.offset+1])
		return errorBinarySize
	}
	//recheck size
	if reader.Size < reader.offset+8 {
		return errorBinarySize
	}

	//pass 2 sign and 2 version bytes
	reader.offset += 4
	len := reader.ReadLen()
	if reader.Size < len+reader.offset {
		return 1
	}
	timePos := reader.ReadTime()
	beginUnix := reader.BeginTime.Unix() * 1000
	endUnix := reader.EndTime.Unix() * 1000
	if (reader.BeginTime.IsZero() && reader.EndTime.IsZero()) || (int64(timePos) >= beginUnix && int64(timePos) <= endUnix) {
		reader.newFlatPosition(timePos)
		reader.offset += 8
		len -= 8
		var res int16
		// reader.pos = new(BinaryPosition)
		// reader.pos.Init()
		for reader.Size >= len && len >= 3 {
			startOffset := reader.offset
			key := reader.ReadKey()
			kind := reader.ReadKind()
			if (kind & binaryArrayEvents) != 0 {
				res = reader.ReadArrayEvents(binaryUint16)
			} else {
				res = reader.ReadFlatValue(kind, key)
			}
			if res < 0 {
				return res
			}
			len -= reader.offset - startOffset
		}
	} else {
		reader.offset += len - 1
		return -1
	}

	return 0
}

// newFlatPosition for old protocol
func (r *BinaryReader) newFlatPosition(time float64) *FlatPosition {
	r.flatPos = new(FlatPosition)
	r.flatPos.P = make(map[uint16]float64)
	r.flatPos.Time = time
	return r.flatPos
}

// Set buf bytes
func (r *BinaryReader) Set(buf *[]byte) {
	r.Buf = *buf
	r.Reset()
}

// NewReader create new BinaryReader, with reset()
func NewReader() *BinaryReader {
	reader := BinaryReader{}
	reader.Reset()
	return &reader
}

// Scan - database field scan json
func (pos *FlatPosition) Scan(src interface{}) error {
	val, ok := src.([]byte)
	if !ok {
		log.Warn("unable scan flat pos:", src)
		return errors.New("unable scan flat pos")
	}
	err := json.Unmarshal(val, &pos)
	if err != nil {
		return err
	}
	return nil
}

// TmtField is a base struct for filter params
type TmtField struct {
	Field string
	Code  uint16
	Value float64
}

// TmtFilter is a base struct for parsing filters
type TmtFilter struct {
	FilterType string
	Pos        []TmtField
}

// ParseTelemetryParams parses filtered params from URL
func ParseTelemetryParams(filtered string) (res []TmtFilter) {
	filters := strings.Split(filtered, "$")
	for _, kv := range filters {
		if kv == "" {
			continue
		}
		filterArray := []TmtField{}
		keyValue := strings.Split(kv, "->")
		if len(keyValue) < 2 {
			continue
		}
		filterType := keyValue[0]
		f := strings.Split(keyValue[1], "~")
		if len(f) < 2 {
			continue
		}
		valueStr := f[1]
		numFields := strings.Split(f[0], ",")
		lenMultiFields := len(numFields)
		if lenMultiFields > 1 {
			for i := 0; i < lenMultiFields; i++ {
				multiField := numFields[i]
				fieldCode := strings.Split(multiField, ".")
				if len(fieldCode) < 2 {
					continue
				}
				field := fieldCode[0]
				codeStr := fieldCode[1]
				code, err := strconv.Atoi(codeStr)
				if err != nil {
					continue
				}
				v, err := strconv.ParseFloat(valueStr, 64)
				if err != nil {
					continue
				}
				filterArray = append(filterArray, TmtField{
					Field: field,
					Code:  uint16(code),
					Value: v,
				})
			}
		} else if len(numFields) > 0 {
			fieldCode := strings.Split(numFields[0], ".")
			if len(fieldCode) < 2 {
				continue
			}
			field := fieldCode[0]
			codeStr := fieldCode[1]
			code, err := strconv.Atoi(codeStr)
			if err != nil {
				continue
			}
			v, err := strconv.ParseFloat(valueStr, 64)
			if err != nil {
				continue
			}
			filterArray = append(filterArray, TmtField{
				Field: field,
				Code:  uint16(code),
				Value: v,
			})
		}
		res = append(res, TmtFilter{
			FilterType: filterType,
			Pos:        filterArray,
		})
	}
	return res
}

// CheckFilter checks object position by provided filters
func (pos *FlatPosition) CheckFilter(filters []TmtFilter) bool {
	numFilters := len(filters)
	isChecked := false
	for i := 0; i < numFilters; i++ {
		filter := filters[i]
		numFilterParams := len(filter.Pos)
		if numFilterParams > 1 {
			for j := 0; j < numFilterParams; j++ {
				filteredParam := filter.Pos[j]
				isChecked = pos.CheckCondition(filter.FilterType, filteredParam.Field, filteredParam.Code, filteredParam.Value)
				if isChecked {
					break
				}
			}
		} else if numFilterParams > 0 {
			filteredParam := filter.Pos[0]
			isChecked = pos.CheckCondition(filter.FilterType, filteredParam.Field, filteredParam.Code, filteredParam.Value)
		}
	}
	return isChecked
}

// CheckCondition checks field of position by code , filterType and value
func (pos *FlatPosition) CheckCondition(filterType, field string, code uint16, v float64) bool {
	var comparedValue float64
	var ok bool
	switch field {
	case "p":
		comparedValue, ok = pos.P[code]
		if !ok {
			return false
		}
		break
	case "t":
		comparedValue = pos.Time
		break
	case "z":
		break
	}

	switch filterType {
	case "eq":
		return comparedValue == v
	case "noteq":
		return comparedValue != v
	case "mask":
		return int64(comparedValue)&int64(v) > 0
	case "notMask":
		return int64(comparedValue)&int64(v) == 0
	case "lte":
		return comparedValue <= v
	case "lten":
		return comparedValue <= v || !ok
	case "gte":
		return comparedValue >= v
	case "gten":
		return comparedValue >= v || ok
	case "lt":
		return comparedValue < v
	case "gt":
		return comparedValue > v
	}
	return false
}

// GetValueFromPosition returns value of param code passed in str and getted from fields
func GetValueFromPosition(fields map[string]interface{}, str string) (interface{}, error) {
	parts := strings.Split(str, ".")
	if len(parts) < 3 {
		return nil, errors.New("not enough arguments")
	}
	positionInt, ok := fields["position"]
	if !ok {
		return nil, errors.New("position field is missing")
	}
	position, ok := positionInt.(*FlatPosition)
	if !ok {
		return nil, errors.New("err conv position field to FlatPosition")
	}
	if position == nil {
		return nil, errors.New("position is nil")
	}
	if parts[1] == "p" {
		paramCodeInt64, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return nil, err
		}
		paramCode := uint16(paramCodeInt64)
		return position.P[paramCode], nil
	}
	return nil, nil
}

func (reader *BinaryReader) ReadFlatValue(kind uint8, key uint16) int16 {
	bufLen := uint32(len(reader.Buf))
	nibble := uint32(kind & 0x0F)
	if bufLen < reader.offset+nibble {
		return errorBinaryLen
	}
	if kind != binaryArrayEvents && reader.lenParams > 0 && !reader.Params[key] {
		reader.Skip(kind, key)
		return 0
	}
	switch kind {
	case binaryArrayEvents:
		val := reader.ReadUint16()
		reader.flatPos.E = append(reader.flatPos.E, val)
		if reader.lenEvents > 0 && reader.Events[val] {
			reader.pass = true
		}
	case binaryZero:
		reader.flatPos.P[key] = float64(0)
	case binaryInt8:
		reader.flatPos.P[key] = float64(reader.ReadInt8())
	case binaryUint8:
		reader.flatPos.P[key] = float64(reader.ReadUint8())
	case binaryInt16:
		reader.flatPos.P[key] = float64(reader.ReadInt16())
	case binaryUint16:
		reader.flatPos.P[key] = float64(reader.ReadUint16())
	case binaryInt32:
		reader.flatPos.P[key] = float64(reader.ReadInt32())
	case binaryUint32:
		reader.flatPos.P[key] = float64(reader.ReadUint32())
	case binaryFloat64:
		if mapParamsPrecision[key] > 0 {
			reader.flatPos.P[key] = math.Round(float64(reader.ReadFloat64())*mapParamsPrecision[key]) / mapParamsPrecision[key]
		} else {
			reader.flatPos.P[key] = float64(reader.ReadFloat64())
		}
	case binaryFloat32:
		if mapParamsPrecision[key] > 0 {
			p := reader.ReadFloat32()
			// log.Debug("old:", p)
			reader.flatPos.P[key] = math.Round(float64(p)*mapParamsPrecision[key]) / mapParamsPrecision[key]
			// log.Debug("new", reader.flatPos.P[key])
		} else {
			reader.flatPos.P[key] = float64(reader.ReadFloat32())
		}
	}

	return 0
}

// UTC translate position time to Time UTC
func (pos *FlatPosition) UTC() time.Time {
	sec := pos.Time / 1000
	nsec := (sec - math.Floor(sec)) * 1000000000
	return time.Unix(int64(sec), int64(nsec)).UTC()
}

// TimeStr format position time
func (pos *FlatPosition) TimeStr() string {
	return time.Unix(int64(pos.Time/1000), 0).Format("2006-01-02 15:04:05")
}
