package csxegts

import "github.com/kuznetsovin/egts-protocol/app/egts"

func newServiceInfoData(serviceType byte, serviceRoutingPriority string) *SrServiceInfo {
	data := SrServiceInfo{
		ServiceType:            serviceType,
		ServiceStatement:       EgtsSstInService,
		ServiceAttribute:       SaSupported,
		ServiceRoutingPriority: serviceRoutingPriority,
	}

	return &data
}

func CreateServiceInfoPacket() (*Packet, uint16) {
	recordData := []subrecordData{
		// {SubrecordType: SrServiceInfoType, SubrecordData: newServiceInfoData(egts.AuthService, SrPriorityHighest)}, // not necessary
		{SubrecordType: SrServiceInfoType, SubrecordData: newServiceInfoData(egts.TeledataService, SrPriorityHigh)},
	}
	authFrameData, recNum := newServiceFrameData(nil, RpPriorityHigh, egts.AuthService, recordData)

	return newPacket(PacketIDCounter.Next(), egts.PtAppdataPacket, PacketPriorityHigh, authFrameData), recNum
}
