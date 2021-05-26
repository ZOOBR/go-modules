package csxbinary

import (
	"encoding/binary"
	"math"

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

// BinaryReader base struct for read binary data
type BinaryReader struct {
	Buf      []byte
	offset   uint32
	Size     uint32
	lenElems int
	// Data     map[string]interface{}
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

// NewReader create new BinaryReader, with reset()
func NewReader() *BinaryReader {
	reader := BinaryReader{}
	reader.Reset()
	return &reader
}

// Set buf bytes
func (r *BinaryReader) Set(buf *[]byte) {
	r.Buf = *buf
	r.Reset()
}

// ReadUint64 bin to uint64
func ReadUint64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}

// ReadUint32 bin to uint32
func ReadUint32(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf)
}

// ReadUint16 bin to uint16
func ReadUint16(buf []byte) uint16 {
	return binary.BigEndian.Uint16(buf)
}

// ReadFloat32 bin to float32
func ReadFloat32(buf []byte) float32 {
	return math.Float32frombits(binary.BigEndian.Uint32(buf))
}

// ReadFloat64 bin to float64
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

// Reset write len's to struct and reset offset
func (r *BinaryReader) Reset() {
	r.offset = 0
	r.Size = uint32(len(r.Buf))
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

// ReadUint8 read number
func (r *BinaryReader) ReadUint8() uint8 {
	return uint8(r.ReadInt8())
}

// ReadInt16 read number
func (r *BinaryReader) ReadInt16() int16 {
	v := int16(ReadUint16(r.Buf[r.offset : r.offset+2]))
	r.offset += 2
	return v
}

// ReadUint16 read number
func (r *BinaryReader) ReadUint16() uint16 {
	v := ReadUint16(r.Buf[r.offset : r.offset+2])
	r.offset += 2
	return v
}

// ReadInt32 read number
func (r *BinaryReader) ReadInt32() int32 {
	v := int32(ReadUint32(r.Buf[r.offset : r.offset+4]))
	r.offset += 4
	return v
}

// ReadUint32 read number
func (r *BinaryReader) ReadUint32() uint32 {
	v := ReadUint32(r.Buf[r.offset : r.offset+4])
	r.offset += 4
	return v
}

// ReadFloat32 read number
func (r *BinaryReader) ReadFloat32() float32 {
	bits := binary.BigEndian.Uint32(r.Buf[r.offset : r.offset+4])
	float := math.Float32frombits(bits)
	r.offset += 4
	return float
}

// ReadFloat64 read number
func (r *BinaryReader) ReadFloat64() float64 {
	bits := binary.BigEndian.Uint64(r.Buf[r.offset : r.offset+8])
	float := math.Float64frombits(bits)
	r.offset += 8
	return float
}

func Float64ToByte(f float64) []byte {
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

// getNumberKind check number type
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

// writeNumber conver numbers to bytes
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
		res = Float64ToByte(value)
	}

	*bytes = append(*bytes, byte(kind))
	if len(res) > 0 {
		*bytes = append(*bytes, res...)
	}

}

// ReadLen read length
func (r *BinaryReader) ReadLen() uint32 {
	v := ReadUint32(r.Buf[r.offset : r.offset+4])
	r.offset += 4
	return v
}

// ReadTime read time (old p.)
func (r *BinaryReader) ReadTime() float64 {
	v := ReadFloat64(r.Buf[r.offset : r.offset+8])
	return v
}

// ReadKey read key (old p.)
func (r *BinaryReader) ReadKey() uint16 {
	v := ReadUint16(r.Buf[r.offset : r.offset+2])
	r.offset += 2
	return v
}

// ReadKind get kind and shift offset
func (r *BinaryReader) ReadKind() uint8 {
	v := uint8(r.Buf[r.offset])
	r.offset++
	return v
}

// Skip slide by element based on type
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
	sign := []byte{98, 116, 0, protocolVersion2}
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

// ReadData convert binary to map and save to BinaryReader.Data if you wanna read one element, use reset()
func (r *BinaryReader) ReadData() (map[string]interface{}, int16) {
	// init result variable
	result := make(map[string]interface{})
	//check size
	if r.Size < 8 {
		return nil, errorBinarySize
	}
	//check sign and pass invalid data
	if !r.CheckSign() {
		return nil, errorBinarySize
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
			return nil, errorBinaryRead
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
			return nil, errorBinaryRead
		}
	}

	return result, 0
}

// ReadAllData sequential reading of all data
func (r *BinaryReader) ReadAllData() ([]map[string]interface{}, int16) {
	r.Reset()
	var result []map[string]interface{}
	for r.Size > r.offset {
		item, err := r.ReadData()
		if err != 0 {
			return nil, err
		}
		result = append(result, item)
	}

	return result, 0
}
