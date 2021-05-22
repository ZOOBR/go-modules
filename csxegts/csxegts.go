package csxegts

import (
	"github.com/kuznetsovin/egts-protocol/app/egts"
)

var (
	PacketIDCounter     = Counter{}
	RecordNumberCounter = Counter{}
)

func newRecord(recordType byte, data egts.BinaryData) *egts.RecordData {
	r := egts.RecordData{
		SubrecordType:   recordType,
		SubrecordLength: data.Length(),
		SubrecordData:   data,
	}

	return &r
}
