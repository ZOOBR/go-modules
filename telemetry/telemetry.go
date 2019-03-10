package telemetry

import (
	"encoding/binary"
	"math"
	"time"

	log "github.com/sirupsen/logrus"
)

var mapParamsPrecision = map[uint16]float64{
	1021: 100,
	1022: 100,
	1101: 1000000000,
	1102: 1000000000,
	1201: 1000,
}

var telemetryParams = map[uint16]string{
	1: "ParamTime",
	2: "ParamEvent",

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

type Position struct {
	Time int64 `json:"time"`
}

type ParamsFloat32 struct {
	K uint16
	V float32
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
	I16  []ParamsInt16
	UI32 []ParamsUint32
	E    []uint16
}

type FlatPosition struct {
	Time float64            `json:"t"`
	P    map[uint16]float64 `json:"p"`
	E    []uint16           `json:"e"`
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
}

type BinaryData struct {
	Day  string
	Time float64
	Data []byte
}

func TranslatePos(p *FlatPosition) map[string]interface{} {
	tPos := make(map[string]interface{})
	for c, v := range p.P {
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
	if protocolVersion != ReadUint16(r.Buf[offset:offset+2]) {
		return false
	}
	return true
}

func (r *BinaryReader) ReadArray(kind uint8) bool {
	bufLen := uint32(len(r.Buf))
	nibble := uint32(kind & 0x0F)
	if bufLen < r.offset+2 {
		return true
	}
	elCount := uint32(ReadUint16(r.Buf[r.offset : r.offset+2]))
	r.offset += 2
	if bufLen < r.offset+elCount*nibble {
		return true
	}
	if r.PositionFormat == "flat" {
		r.flatPos.E = make([]uint16, elCount)
	} else {
		r.pos.E = make([]uint16, elCount)
	}

	for index := uint32(0); index < elCount; index++ {
		key := uint16(index)
		if r.lenEvents != 0 && !r.Events[key] {
			continue
		}
		err := r.ReadValue(binaryArray, uint16(index))
		if err {
			return true
		}
	}
	return false
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
	r.offset += 2
	return v
}

func (r *BinaryReader) ReadUint32() uint32 {
	v := ReadUint32(r.Buf[r.offset : r.offset+4])
	r.offset += 2
	return v
}

func (r *BinaryReader) ReadFloat32() float32 {
	bits := binary.BigEndian.Uint32(r.Buf[r.offset : r.offset+4])
	float := math.Float32frombits(bits)
	r.offset += 4
	return float
}

func (r *BinaryReader) ReadFloat64() float64 {
	bits := binary.BigEndian.Uint64(r.Buf[r.offset : r.offset+4])
	float := math.Float64frombits(bits)
	r.offset += 8
	return float
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
	}
}

func (reader *BinaryReader) ReadValue(kind uint8, key uint16) bool {
	if reader.PositionFormat == "flat" {
		return reader.ReadFlatValue(kind, key)
	}
	bufLen := uint32(len(reader.Buf))
	nibble := uint32(kind & 0x0F)
	if bufLen < reader.offset+nibble {
		return true
	}
	if kind != binaryArray && reader.lenParams > 0 && !reader.Params[key] {
		reader.Skip(kind, key)
		return false
	}
	switch kind {
	case binaryArray:
		reader.pos.E[key] = reader.ReadUint16()
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
	}

	return false
}

func (reader *BinaryReader) ReadFlatValue(kind uint8, key uint16) bool {
	bufLen := uint32(len(reader.Buf))
	nibble := uint32(kind & 0x0F)
	if bufLen < reader.offset+nibble {
		return true
	}
	if kind != binaryArray && reader.lenParams > 0 && !reader.Params[key] {
		reader.Skip(kind, key)
		return false
	}
	switch kind {
	case binaryArray:
		reader.flatPos.E[key] = reader.ReadUint16()
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
	case binaryFloat32:
		if mapParamsPrecision[key] > 0 {
			reader.flatPos.P[key] = math.Round(float64(reader.ReadFloat32())*mapParamsPrecision[key]) / mapParamsPrecision[key]
		} else {
			reader.flatPos.P[key] = float64(reader.ReadFloat32())
		}
	}

	return false
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
		if res != 0 {
			reader.offset++
			continue
		}
		posArr = append(posArr, *reader.pos)
	}
	return 0, posArr
}

func (reader *BinaryReader) ReadFlatPositions() (int16, []FlatPosition) {
	reader.PositionFormat = "flat"
	reader.Reset()
	posArr := make([]FlatPosition, 0)
	var currPos *FlatPosition
	for reader.Size > reader.offset {
		res := reader.readPosition()
		if res != 0 {
			reader.offset++
			continue
		}
		if currPos != reader.flatPos {
			posArr = append(posArr, *reader.flatPos)
		}
	}
	return 0, posArr
}

func (reader *BinaryReader) readPosition() int16 {

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
	beginUnix := reader.BeginTime.UnixNano() / int64(time.Millisecond)
	endUnix := reader.EndTime.UnixNano() / int64(time.Millisecond)
	if (reader.BeginTime.IsZero() && reader.EndTime.IsZero()) || (int64(timePos) >= beginUnix && int64(timePos) <= endUnix) {
		if reader.PositionFormat == "struct" {
			reader.newPosition(timePos)
		} else {
			reader.newFlatPosition(timePos)
		}
		reader.offset += 8
		len -= 8
		var err bool
		reader.pos = new(BinaryPosition)
		reader.pos.Init()
		for reader.Size >= len && len >= 3 {
			startOffset := reader.offset
			key := reader.ReadKey()
			kind := reader.ReadKind()
			if (kind & binaryArray) != 0 {
				err = reader.ReadArray(binaryUint16)
			} else {
				err = reader.ReadValue(kind, key)
				if err {
					break
				}
			}
			if err {
				return 1
			}

			len -= reader.offset - startOffset
		}
	} else {
		reader.offset += len
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
