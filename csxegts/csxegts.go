package csxegts

import (
	"encoding/binary"

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

func newServiceDataRecord(objectIdentifier *uint32, priority string, serviceType byte, record *egts.RecordData) *egts.ServiceDataRecord {
	rds := egts.RecordDataSet{*record}

	sdr := egts.ServiceDataRecord{
		RecordLength:             rds.Length(),
		RecordNumber:             RecordNumberCounter.Next(),
		SourceServiceOnDevice:    SsodTerminal,
		RecipientServiceOnDevice: RsodPlatform,
		Group:                    string(priority[0]), // this field was removed from standart. Delete this field & rework to priority after EGTS lib updating
		RecordProcessingPriority: priority[1:],
		TimeFieldExists:          "1",
		EventIDFieldExists:       "0",
		Time:                     EgtsTimeNowSeconds(), //TODO: rewrite to time.Now() after EGTS lib is updated
		SourceServiceType:        serviceType,
		RecipientServiceType:     serviceType,
		RecordDataSet:            rds,
	}
	if objectIdentifier != nil {
		sdr.ObjectIDFieldExists = "1"
		sdr.ObjectIdentifier = *objectIdentifier
	} else {
		sdr.ObjectIDFieldExists = "0"
	}

	return &sdr
}

func newServiceFrameData(objectIdentifier *uint32, priority string, serviceType byte, recordType byte, recordData egts.BinaryData) (*egts.ServiceDataSet, uint16) {
	record := newRecord(recordType, recordData)
	sdr := newServiceDataRecord(objectIdentifier, priority, serviceType, record)

	return &egts.ServiceDataSet{*sdr}, sdr.RecordNumber
}

// newPacket returns EGTS packet
func newPacket(packetID uint16, packetType byte, priority string, peerAddress, recipientAddress *uint16, servicesFrameData egts.BinaryData) *Packet {
	packet := Packet{
		egts.Package{
			ProtocolVersion:   egtsHeaderProtocolVersion,
			SecurityKeyID:     0,
			Prefix:            egtsHeaderPrefix,
			EncryptionAlg:     "00",
			Compression:       "0",
			Priority:          priority,
			HeaderEncoding:    0,
			FrameDataLength:   servicesFrameData.Length(),
			PacketIdentifier:  packetID,
			PacketType:        packetType,
			ServicesFrameData: servicesFrameData,
		},
	}

	if peerAddress != nil && recipientAddress != nil {
		packet.Route = "1"
		packet.PeerAddress = *peerAddress
		packet.RecipientAddress = *recipientAddress
		packet.TimeToLive = 64
		packet.HeaderLength = 16
	} else {
		packet.Route = "0"
		packet.HeaderLength = 11
	}

	return &packet
}

// IsEGTSPacket checks whether the byte array is an EGTS packet
func IsEGTSPacket(rawPacket []byte) bool {
	return rawPacket[0] == egtsHeaderProtocolVersion
}

func GetPacketLength(header []byte) uint16 {
	bodyLength := binary.LittleEndian.Uint16(header[5:7])
	packetLength := uint16(header[3])
	if bodyLength > 0 {
		packetLength += bodyLength + 2
	}
	return packetLength
}

func DecodePacket(raw []byte) (*Packet, *uint8, error) {
	packet := Packet{}
	n, err := packet.Decode(raw)
	if err != nil {
		return nil, nil, err
	}
	return &packet, &n, nil
}
