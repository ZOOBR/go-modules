package telemetry

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
	// base
	ParamLat      = 1101
	ParamLon      = 1102
	ParamAngle    = 1104
	ParamOdometer = 1201
	// can
	ParamCanStatus = 2200
	ParamCanSpeed  = 2202
	// calculated
	ParamCalcOdo  = 3001
	ParamSpeedAvg = 3005

	ParamClientLat = 4101
	ParamClientLon = 4102
)

var mapParamsPrecision = map[uint16]float64{
	1021: 100,
	1022: 100,
	1101: 1000000000,
	1102: 1000000000,
	1201: 1000,
}

var telemetryParams = map[uint16]string{
	1:  "ParamTime",
	10: "FuelTime1",
	11: "FuelTime2",
	2:  "ParamEvent",

	// System parameters
	1020: "ParamDataStatus",
	1021: "ParamDataPower",
	1022: "ParamDataBattery",
	1023: "ParamDataGSM",
	1024: "ParamDataGPS",

	// GPS parameters
	1101: "ParamDataLat",
	1102: "ParamDataLon",
	1103: "ParamDataAlt",
	1104: "ParamDataHead",
	1105: "ParamDataSpeed",

	// Drive parameters
	1201: "ParamDataOdo",
	1221: "ParamDataDriver",
	1222: "ParamDataZone",

	// Binary sensor parameters
	1901: "ParamDataInput",
	1902: "ParamDataOutput",

	// Other parameter groups (with index)
	2000: "ParamDataAnalog1",
	2001: "ParamDataAnalog2",
	2002: "ParamDataAnalog3",
	2003: "ParamDataAnalog4",
	2004: "ParamDataAnalog5",
	2010: "ParamDataDigit1",
	2011: "ParamDataDigit2",
	2012: "ParamDataDigit3",
	2013: "ParamDataDigit4",
	2014: "ParamDataDigit5",
	2020: "ParamDataMoto1",
	2021: "ParamDataMoto2",
	2022: "ParamDataMoto3",
	2023: "ParamDataMoto4",
	2024: "ParamDataMoto5",
	2100: "ParamDataParams1",
	2101: "ParamDataParams2",
	2102: "ParamDataParams3",
	2103: "ParamDataParams4",
	2104: "ParamDataParams5",
	2200: "ParamDataCan",
	2300: "ParamDataTire1",
	2301: "ParamDataTire2",
	2302: "ParamDataTire3",
	2303: "ParamDataTire4",
	2400: "ParamDataTempr1",
	2401: "ParamDataTempr2",
	2402: "ParamDataTempr3",

	// Single parameters codes
	// 2200: "ParamCanStatus",
	2201: "ParamCanTempr",
	2202: "ParamCanSpeed",
	2203: "ParamCanOdometer",
	2204: "ParamCanFuelTotal",
	2205: "ParamCanMotoTotal",
	2206: "ParamCanFuelLevel",
	2207: "ParamCanAccel",
	2208: "ParamCanTacho",
	2209: "ParamCanBrake",
	2250: "ParamDataPosNum"}

const (
	//binary protocol constants
	protocolVersion    = 0
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
	binaryArray        = 0x40
	paramEvent         = 2
	//struct type
	structInt16     = 0
	structUint32    = 1
	structF32       = 2
	errorBinarySize = 303
	errorBinaryLen  = 304
	errorProtoTime  = 305
	errorBinaryRead = 306
)

var (
	binaryID = []rune("bt")
)

// PositionInfo - position info (base check content)
type PositionInfo struct {
	Time float64       `json:"time"`
	ID   string        `json:"id"`
	Type string        `json:"type"`
	Data *FlatPosition `json:"data"`
	Auth *Auth         `json:"auth"`
}

// Auth - auth token info
type Auth struct {
	Token   string  `json:"token"`
	Expired float64 `json:"expired"`
}

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

type BinaryPosition struct {
	Time float64
	F32  []ParamsFloat32
	F64  []ParamsFloat64
	I16  []ParamsInt16
	UI32 []ParamsUint32
	E    []uint16
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

// PrettyPosition struct for user friendly
type PrettyPosition struct {
	Time    float64  `json:"t"`  //[PARAMS.time, 't'],
	Events  []uint16 `json:"e"`  //[PARAMS.event, 'e'],
	Status  float64  `json:"st"` //[PARAMS.status, 'st'],
	Power   float64  `json:"v"`  //[PARAMS.power, 'v'],
	Battery float64  `json:"bv"` //[PARAMS.battery, 'bv'],
	Gsm     int8     `json:"sl"` //[PARAMS.gsm, 'sl'],
	Gps     int8     `json:"sc"` //[PARAMS.gps, 'sc'],
	Lon     float64  `json:"x"`  //[PARAMS.lon, 'x'],
	Lat     float64  `json:"y"`  //[PARAMS.lat, 'y'],
	Alt     float64  `json:"z"`  //[PARAMS.alt, 'z'],
	Speed   float64  `json:"s"`  //[PARAMS.speed, 's'],
	Angle   int8     `json:"a"`  //[PARAMS.dir, 'a'],
	Odo     float64  `json:"d"`  //[PARAMS.odo, 'd'],
	Sensor  float64  `json:"sm"` //[PARAMS.sensor, 'sm'],
	Relay   float64  `json:"rm"` //[PARAMS.relay, 'rm'],

	// groups (with index)
	Analog map[uint16]float64 `json:"an"` //[PARAMS.analog, 'an'],
	Digit  map[uint16]float64 `json:"dg"` //[PARAMS.digit, 'dg'],
	Moto   map[uint16]float64 `json:"m"`  //[PARAMS.moto, 'm'],
	Tempr  map[uint16]float64 `json:"tp"` //[PARAMS.tempr, 'tp'],
	Can    map[uint16]float64 `json:"c"`  //[PARAMS.can, 'c'],
	Tire   map[uint16]float64 `json:"w"`  //[PARAMS.tire, 'w']
	// other params
	P map[uint16]float64 `json:"p"` //[PARAMS.p, 'p']
}

type BinaryReader struct {
	Buf            []byte
	offset         uint32
	Size           uint32
	Params         map[uint16]bool
	Events         map[uint16]bool
	BeginTime      time.Time
	EndTime        time.Time
	lenEvents      int
	lenParams      int
	PositionFormat string
	pos            *BinaryPosition
	flatPos        *FlatPosition
	pass           bool
}

type BinaryData struct {
	Day  string
	Time float64
	Data []byte
}

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

	"paramClientLat": {4101, 0, true},
	"paramClientLon": {4102, 0, true},
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

func TranslatePos(p *FlatPosition) map[string]interface{} {
	tPos := make(map[string]interface{})
	for c, v := range p.P {
		if mapParamsPrecision[c] > 0 {
			v = math.Round(float64(v)*mapParamsPrecision[c]) / mapParamsPrecision[c]
		}
		tPos[telemetryParams[c]] = v
	}
	return tPos
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

func (pos *BinaryPosition) Init() {
	pos.I16 = make([]ParamsInt16, 0)
	pos.UI32 = make([]ParamsUint32, 0)
	pos.F32 = make([]ParamsFloat32, 0)
}

func (r *BinaryReader) CheckSign() bool {
	if r.Buf[r.offset] != uint8(binaryID[0]) || (r.Buf)[r.offset+1] != uint8(binaryID[1]) {
		return false
	}
	return true
}

func (r *BinaryReader) CheckVersion() bool {
	offset := 2
	packetProtocolVersion := ReadUint16(r.Buf[offset : offset+2])
	return protocolVersion == packetProtocolVersion
}

func (r *BinaryReader) ReadArray(kind uint8) int16 {
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
	if r.PositionFormat == "flat" {
		r.flatPos.E = make([]uint16, 0)
	} else {
		r.pos.E = make([]uint16, 0)
	}

	for index := uint32(0); index < elCount; index++ {
		err := r.ReadValue(binaryArray, uint16(index))
		if err != 0 {
			continue
		}
	}
	return 0
}

func (r *BinaryReader) Reset() {
	r.offset = 0
	r.Size = uint32(len(r.Buf))
	r.lenEvents = len(r.Events)
	r.lenParams = len(r.Params)
}

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

// func float64ToByte(f float64) []byte {
// 	var buf [8]byte
// 	binary.BigEndian.PutUint64(buf[:], math.Float64bits(f))
// 	return buf[:]
// }

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

func (reader *BinaryReader) ReadValue(kind uint8, key uint16) int16 {
	if reader.PositionFormat == "flat" {
		return reader.ReadFlatValue(kind, key)
	}
	bufLen := uint32(len(reader.Buf))
	nibble := uint32(kind & 0x0F)
	if bufLen < reader.offset+nibble {
		return errorBinaryLen
	}
	if kind != binaryArray && reader.lenParams > 0 && !reader.Params[key] {
		reader.Skip(kind, key)
		return 0
	}
	switch kind {
	case binaryArray:
		val := reader.ReadUint16()
		reader.pos.E = append(reader.pos.E, val)
		if reader.lenEvents > 0 && reader.Events[val] {
			reader.pass = true
		}
	case binaryZero:
		reader.pos.I16 = append(reader.pos.I16, ParamsInt16{key, 0})
	case binaryInt8:
		reader.pos.I16 = append(reader.pos.I16, ParamsInt16{key, int16(reader.ReadInt8())})
	case binaryUint8:
		reader.pos.I16 = append(reader.pos.I16, ParamsInt16{key, int16(reader.ReadUint8())})
	case binaryInt16:
		reader.pos.I16 = append(reader.pos.I16, ParamsInt16{key, reader.ReadInt16()})
	case binaryUint16:
		reader.pos.UI32 = append(reader.pos.UI32, ParamsUint32{key, uint32(reader.ReadUint16())})
	case binaryInt32:
		reader.pos.I16 = append(reader.pos.I16, ParamsInt16{key, int16(reader.ReadInt32())})
	case binaryUint32:
		reader.pos.UI32 = append(reader.pos.UI32, ParamsUint32{key, reader.ReadUint32()})
	case binaryFloat32:
		reader.pos.F32 = append(reader.pos.F32, ParamsFloat32{key, reader.ReadFloat32()})
	case binaryFloat64:
		reader.pos.F64 = append(reader.pos.F64, ParamsFloat64{key, reader.ReadFloat64()})
	}

	return 0
}

func (reader *BinaryReader) ReadFlatValue(kind uint8, key uint16) int16 {
	bufLen := uint32(len(reader.Buf))
	nibble := uint32(kind & 0x0F)
	if bufLen < reader.offset+nibble {
		return errorBinaryLen
	}
	if kind != binaryArray && reader.lenParams > 0 && !reader.Params[key] {
		reader.Skip(kind, key)
		return 0
	}
	switch kind {
	case binaryArray:
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

func (reader *BinaryReader) ReadBinaryPosition() (uint16, string, []byte, float64) {
	if reader.Size < reader.offset+8 {
		return errorBinarySize, "", nil, 0.00
	}
	startOffset := reader.offset
	reader.offset += 4
	len := reader.ReadLen()
	if reader.Size < len+reader.offset {
		return 0, "", nil, 0
	}
	posTime := reader.ReadTime()
	t := time.Unix(int64(posTime)/1000, 0)
	day := t.Format("20060102")
	reader.offset += len
	result := reader.Buf[startOffset:reader.offset]
	return 0, day, result, posTime
}

func (reader *BinaryReader) ReadStructPositions() (int16, []BinaryPosition) {
	reader.PositionFormat = "struct"
	reader.Reset()
	posArr := make([]BinaryPosition, 0)
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
			posArr = append(posArr, *reader.pos)
		}
	}
	return 0, posArr
}

func (reader *BinaryReader) ReadFlatPositions() ([]FlatPosition, int16) {
	reader.PositionFormat = "flat"
	reader.Reset()
	posArr := make([]FlatPosition, 0)
	for reader.Size > reader.offset {
		res := reader.readPosition()
		if res < 0 {
			reader.offset++
			continue
		}
		if res == errorBinarySize || res == errorBinaryLen || res == errorBinaryRead || res == errorProtoTime {
			return posArr, res
		}
		if reader.flatPos != nil && (reader.lenEvents == 0 || reader.pass) {
			posArr = append(posArr, *reader.flatPos)
		}
	}
	return posArr, 0
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
		data = append(data, byte(binaryArray|binaryUint16))
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
		if reader.PositionFormat == "struct" {
			reader.newPosition(timePos)
		} else {
			reader.newFlatPosition(timePos)
		}
		reader.offset += 8
		len -= 8
		var res int16
		reader.pos = new(BinaryPosition)
		reader.pos.Init()
		for reader.Size >= len && len >= 3 {
			startOffset := reader.offset
			key := reader.ReadKey()
			kind := reader.ReadKind()
			if (kind & binaryArray) != 0 {
				res = reader.ReadArray(binaryUint16)
			} else {
				res = reader.ReadValue(kind, key)
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

func (reader *BinaryReader) ReadBinaryPositions() (err uint16, result []BinaryData) {
	var curDay string
	var posData []byte
	var currPos BinaryData
	reader.Reset()
	for reader.Size > reader.offset {
		err, day, newData, posTime := reader.ReadBinaryPosition()
		if err != 0 {
			log.Warn("error code:", err)
			continue
		}
		if curDay != day && curDay != "" {
			currPos = BinaryData{curDay, posTime, posData}
			result = append(result, currPos)
		} else if reader.Size <= reader.offset {
			posData = append(posData, newData...)
			currPos = BinaryData{day, posTime, posData}
			result = append(result, currPos)
			break
		}
		posData = append(posData, newData...)
	}
	return 0, result
}

func (reader *BinaryReader) newPosition(time float64) *BinaryPosition {
	reader.pos = new(BinaryPosition)
	reader.pos.Init()
	reader.pos.Time = time
	return reader.pos
}

func (reader *BinaryReader) newFlatPosition(time float64) *FlatPosition {
	reader.flatPos = new(FlatPosition)
	reader.flatPos.P = make(map[uint16]float64)
	reader.flatPos.Time = time
	return reader.flatPos
}

func (reader *BinaryReader) Set(buf *[]byte) {
	reader.Buf = *buf
	reader.Reset()
}

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

// Scan - database field scan json
func (auth *Auth) Scan(src interface{}) error {
	val, ok := src.([]byte)
	if !ok {
		log.Warn("unable scan auth:", src)
		return errors.New("unable scan auth")
	}
	err := json.Unmarshal(val, &auth)
	if err != nil {
		return err
	}
	return nil
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

// CopyTo position full or partial
func (pos *FlatPosition) CopyTo(newPos *FlatPosition, extinfo bool) *FlatPosition {
	if newPos == nil {
		newPos = new(FlatPosition)
	}
	newPos.Time = pos.Time
	newPos.P = make(map[uint16]float64)
	for key, val := range pos.P {
		newPos.P[key] = val
	}
	newPos.E = make([]uint16, len(pos.E))
	copy(newPos.E, pos.E)
	if extinfo {
		// Copy as constant reference
		if len(pos.Zones) > 0 {
			newPos.Zones = pos.Zones
		}
	}
	return newPos
}

// Copy position full or partial
func (pos *FlatPosition) Copy(extinfo bool) *FlatPosition {
	return pos.CopyTo(nil, extinfo)
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
