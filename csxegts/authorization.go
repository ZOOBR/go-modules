package csxegts

import "github.com/kuznetsovin/egts-protocol/app/egts"

func newTermIdentityData(imei string) *egts.SrTermIdentity {
	data := egts.SrTermIdentity{
		TerminalIdentifier: 0,
		MNE:                "0",
		BSE:                "0",
		NIDE:               "0",
		SSRA:               "1",
		LNGCE:              "0",
		IMSIE:              "0",
		IMEIE:              "1",
		HDIDE:              "0",
		IMEI:               imei,
		// BufferSize
	}

	return &data
}

func CreateAuthPacket(objectIdentifier uint32, imei string) (*Packet, uint16) {
	recordData := newTermIdentityData(imei)
	authFrameData, recNum := newServiceFrameData(&objectIdentifier, RpPriorityHigh, egts.AuthService, egts.SrTermIdentityType, recordData)

	return newPacket(PacketIDCounter.Next(), egts.PtAppdataPacket, PacketPriorityHigh, authFrameData), recNum
}
