package csxegts

import (
	"bytes"
	"fmt"
	"strconv"
)

// SrDispatcherIdentity is struct for EGTS_SR_SERVICE_INFO subrecord type
// UNUSED
type SrServiceInfo struct {
	ServiceType            byte   `json:"ST"`
	ServiceStatement       byte   `json:"SST"`
	ServiceAttribute       string `json:"SRVA"`
	ServiceRoutingPriority string `json:"SRVRP"`
}

// Decode for BinaryData interface for github.com/kuznetsovin/egts-protocol (and based on it)
func (srsi *SrServiceInfo) Decode(content []byte) error {
	var (
		err   error
		flags byte
	)

	buf := bytes.NewBuffer(content)
	if srsi.ServiceType, err = buf.ReadByte(); err != nil {
		return fmt.Errorf("failed to decode service type: %v", err)
	}
	if srsi.ServiceStatement, err = buf.ReadByte(); err != nil {
		return fmt.Errorf("failed to decode service statement: %v", err)
	}
	if flags, err = buf.ReadByte(); err != nil {
		return fmt.Errorf("failed to decode flags: %v", err)
	}
	flagBits := fmt.Sprintf("%08b", flags)
	srsi.ServiceAttribute = flagBits[:1]       // bit 7
	srsi.ServiceRoutingPriority = flagBits[6:] // bits 0, 1

	return nil
}

// Encode for BinaryData interface for github.com/kuznetsovin/egts-protocol (and based on it)
func (srsi *SrServiceInfo) Encode() ([]byte, error) {
	var (
		result []byte
		err    error
		flags  uint64
	)
	buf := new(bytes.Buffer)

	buf.WriteByte(srsi.ServiceType)
	buf.WriteByte(srsi.ServiceStatement)

	flagsBits := srsi.ServiceAttribute + "00000" + srsi.ServiceRoutingPriority
	if flags, err = strconv.ParseUint(flagsBits, 2, 8); err != nil {
		return result, fmt.Errorf("failed to generate flags: %v", err)
	}
	if err = buf.WriteByte(uint8(flags)); err != nil {
		return result, fmt.Errorf("failed to encode flags: %v", err)
	}

	result = buf.Bytes()
	return result, err
}

// Length for BinaryData interface for github.com/kuznetsovin/egts-protocol (and based on it)
func (srsi *SrServiceInfo) Length() uint16 {
	var result uint16

	if recBytes, err := srsi.Encode(); err != nil {
		result = uint16(0)
	} else {
		result = uint16(len(recBytes))
	}

	return result
}
