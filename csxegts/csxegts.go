package csxegts

import (
	"github.com/kuznetsovin/egts-protocol/app/egts"
)

type Packet struct {
	egts.Package
}

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

// newPacket returns EGTS packet
func newPacket(packetID uint16, packetType byte, priority string, servicesFrameData egts.BinaryData) *Packet {
	packet := Packet{
		egts.Package{
			ProtocolVersion:   egtsHeaderProtocolVersion,
			SecurityKeyID:     0,
			Prefix:            egtsHeaderPrefix,
			Route:             "0",
			EncryptionAlg:     "00",
			Compression:       "0",
			Priority:          priority,
			HeaderLength:      11,
			HeaderEncoding:    0,
			FrameDataLength:   servicesFrameData.Length(),
			PacketIdentifier:  packetID,
			PacketType:        packetType,
			ServicesFrameData: servicesFrameData,
		},
	}

	return &packet
}
