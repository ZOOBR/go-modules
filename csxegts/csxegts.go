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

func newServiceDataRecord(objectIdentifier uint32, priority string, serviceType byte, record *egts.RecordData) *egts.ServiceDataRecord {
	rds := egts.RecordDataSet{*record}

	sdr := egts.ServiceDataRecord{
		RecordLength:             rds.Length(),
		RecordNumber:             RecordNumberCounter.Next(),
		SourceServiceOnDevice:    SsodTerminal,
		RecipientServiceOnDevice: RsodPlatform,
		Group:                    "0", // this field is not found in the standart
		RecordProcessingPriority: priority,
		TimeFieldExists:          "1",
		EventIDFieldExists:       "0",
		ObjectIDFieldExists:      "1",
		ObjectIdentifier:         objectIdentifier,
		Time:                     EgtsTimeNowSeconds(), //TODO: rewrite to time.Now() after EGTS lib is updated
		SourceServiceType:        serviceType,
		RecipientServiceType:     serviceType,
		RecordDataSet:            rds,
	}

	return &sdr
}

func newServiceFrameData(objectIdentifier uint32, priority string, serviceType byte, recordType byte, recordData egts.BinaryData) *egts.ServiceDataSet {
	record := newRecord(recordType, recordData)
	sdr := newServiceDataRecord(objectIdentifier, priority, serviceType, record)

	return &egts.ServiceDataSet{*sdr}
}
