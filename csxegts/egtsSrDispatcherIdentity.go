package csxegts

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// SrDispatcherIdentity is struct for EGTS_SR_DISPATCHER_IDENTITY subrecord type
type SrDispatcherIdentity struct {
	DispatcherType byte   `json:"DT"`
	DispatcherID   uint32 `json:"DID"`
	Description    string `json:"DSCR"` // string length: from 0 to 255
}

// Decode for BinaryData interface for github.com/kuznetsovin/egts-protocol (and based on it)
func (srdi *SrDispatcherIdentity) Decode(content []byte) error {
	var (
		err error
	)
	buf := bytes.NewBuffer(content)

	if srdi.DispatcherType, err = buf.ReadByte(); err != nil {
		return fmt.Errorf("failed to decode dispatcher type: %v", err)
	}

	didBuf := make([]byte, 4)
	if _, err = buf.Read(didBuf); err != nil {
		return fmt.Errorf("failed to decode dispatcher ID: %v", err)
	}
	srdi.DispatcherID = binary.LittleEndian.Uint32(didBuf)

	if srdi.Description, err = buf.ReadString(null); err != nil {
		return fmt.Errorf("failed to decode description: %v", err)
	}

	return nil
}

// Encode for BinaryData interface for github.com/kuznetsovin/egts-protocol (and based on it)
func (srdi *SrDispatcherIdentity) Encode() ([]byte, error) {
	var (
		result []byte
		err    error
	)
	buf := new(bytes.Buffer)

	buf.WriteByte(srdi.DispatcherType) // The returned error is always nil
	if err = binary.Write(buf, binary.LittleEndian, srdi.DispatcherID); err != nil {
		return result, fmt.Errorf("failed to encode dispatcher ID: %v", err)
	}
	if n, _ := buf.WriteString(srdi.Description); n != len(srdi.Description) {
		return result, errors.New("failed to encode description: encoded partially")
	}
	buf.WriteByte(null) // for Description (null-terminated string by standart)

	result = buf.Bytes()
	return result, err
}

// Length for BinaryData interface for github.com/kuznetsovin/egts-protocol (and based on it)
func (srdi *SrDispatcherIdentity) Length() uint16 {
	var result uint16

	if recBytes, err := srdi.Encode(); err != nil {
		result = uint16(0)
	} else {
		result = uint16(len(recBytes))
	}

	return result
}
