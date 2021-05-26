package csxegts

import "github.com/kuznetsovin/egts-protocol/app/egts"

// UNUSED NOW
// func newTermIdentityData() *egts.SrTermIdentity {
// 	data := egts.SrTermIdentity{
// 		TerminalIdentifier: 0,
// 		MNE:                "0",
// 		BSE:                "0",
// 		NIDE:               "0",
// 		SSRA:               "1",
// 		LNGCE:              "0",
// 		IMSIE:              "0",
// 		IMEIE:              "0",
// 		HDIDE:              "0",
// 		// BufferSize
// 	}

// 	return &data
// }

func newDispatcherIdentityData(dispatcherID uint32, description string) *SrDispatcherIdentity {
	data := SrDispatcherIdentity{
		DispatcherType: 0, // this field was not described in standart
		DispatcherID:   dispatcherID,
		Description:    description,
	}

	return &data
}

func CreateAuthPacket(dispatcherID uint32, description string) (*Packet, uint16) {
	recordData := []subrecordData{
		{SubrecordType: egts.SrTermIdentityType, SubrecordData: newDispatcherIdentityData(dispatcherID, description)},
	}
	authFrameData, recNum := newServiceFrameData(nil, RpPriorityHigh, egts.AuthService, recordData)

	return newPacket(PacketIDCounter.Next(), egts.PtAppdataPacket, PacketPriorityHigh, authFrameData), recNum
}
