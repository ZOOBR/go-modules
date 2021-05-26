package csxegts

import "github.com/kuznetsovin/egts-protocol/app/egts"

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

func CreateServiceInfoPacket() (*Packet, uint16) {
	recordData := []subrecordData{
		// {SubrecordType: SrServiceInfoType, SubrecordData: newServiceInfoData(egts.AuthService)}, // not necessary
		{SubrecordType: SrServiceInfoType, SubrecordData: newServiceInfoData(egts.TeledataService)},
	}
	authFrameData, recNum := newServiceFrameData(nil, RpPriorityHigh, egts.AuthService, recordData)

	return newPacket(PacketIDCounter.Next(), egts.PtAppdataPacket, PacketPriorityHigh, authFrameData), recNum
}
