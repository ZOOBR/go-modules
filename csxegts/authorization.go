package csxegts

import "github.com/kuznetsovin/egts-protocol/app/egts"

func newTermIdentityData() *egts.SrTermIdentity {
	data := egts.SrTermIdentity{
		TerminalIdentifier: 0,
		MNE:                "0",
		BSE:                "0",
		NIDE:               "0",
		SSRA:               "1",
		LNGCE:              "0",
		IMSIE:              "0",
		IMEIE:              "0",
		HDIDE:              "0",
		// BufferSize
	}

	return &data
}

func CreateAuthPacket(objectIdentifier uint32) (*Packet, uint16) {
	recordData := newTermIdentityData()
	authFrameData, recNum := newServiceFrameData(&objectIdentifier, RpPriorityHigh, egts.AuthService, egts.SrTermIdentityType, recordData)

	return newPacket(PacketIDCounter.Next(), egts.PtAppdataPacket, PacketPriorityHigh, authFrameData), recNum
}
