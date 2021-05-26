package csxegts

import "github.com/kuznetsovin/egts-protocol/app/egts"

func newDispatcherIdentityData(dispatcherID uint32, description string) *SrDispatcherIdentity {
	data := SrDispatcherIdentity{
		DispatcherType: 0, // this field was not described in standart
		DispatcherID:   dispatcherID,
		Description:    description,
	}

	return &data
}

func newServiceInfoData(serviceType byte) *SrServiceInfo {
	// TODO:: move all constants to params
	data := SrServiceInfo{
		ServiceType:            serviceType,
		ServiceStatement:       0,    // EGTS_SST_IN_SERVICE
		ServiceAttribute:       "0",  // service supported
		ServiceRoutingPriority: "01", // high priority
	}

	return &data
}

func CreateAuthPacket(dispatcherID uint32, description string) (*Packet, uint16) {
	recordData := []subrecordData{
		{SubrecordType: SrDispatcherIdentityType, SubrecordData: newDispatcherIdentityData(dispatcherID, description)},
	}
	authFrameData, recNum := newServiceFrameData(nil, RpPriorityHigh, egts.AuthService, recordData)

	return newPacket(PacketIDCounter.Next(), egts.PtAppdataPacket, PacketPriorityHigh, authFrameData), recNum
}

func CreateServiceInfoPacket() (*Packet, uint16) {
	recordData := []subrecordData{
		// {SubrecordType: SrServiceInfoType, SubrecordData: newServiceInfoData(egts.AuthService)}, // not necessary
		{SubrecordType: SrServiceInfoType, SubrecordData: newServiceInfoData(egts.TeledataService)},
	}
	authFrameData, recNum := newServiceFrameData(nil, RpPriorityHigh, egts.AuthService, recordData)

	return newPacket(PacketIDCounter.Next(), egts.PtAppdataPacket, PacketPriorityHigh, authFrameData), recNum
}
