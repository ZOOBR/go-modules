package csxegts

import "github.com/kuznetsovin/egts-protocol/app/egts"

func newResponseData(confirmedRecordNumber uint16, status uint8) *egts.SrResponse {
	data := egts.SrResponse{
		ConfirmedRecordNumber: confirmedRecordNumber,
		RecordStatus:          status,
	}

	return &data
}

func newResponseServiceFrameData(responsePacketID, confirmedRecordNumber uint16, resultCode, status uint8, serviceType byte) *egts.PtResponse {
	record := newRecord(egts.SrRecordResponseType, newResponseData(confirmedRecordNumber, status))

	sdr := newServiceDataRecord(nil, RpPriorityNormal, serviceType, record)
	sfd := egts.PtResponse{
		ResponsePacketID: responsePacketID,
		ProcessingResult: resultCode,
		SDR:              &egts.ServiceDataSet{*sdr},
	}

	return &sfd
}

func CreateResponsePacket(sourcePacket *Packet, resultCode uint8) *Packet {
	responsePacketID := sourcePacket.PacketIdentifier
	var serviceType byte
	var confirmedRecordNumber uint16
	for _, rec := range *sourcePacket.ServicesFrameData.(*egts.ServiceDataSet) {
		serviceType = rec.SourceServiceType
		confirmedRecordNumber = rec.RecordNumber
		break
	}
	responseFrameData := newResponseServiceFrameData(responsePacketID, confirmedRecordNumber, resultCode, EgtsPcOk, serviceType)

	return newPacket(PacketIDCounter.Next(), egts.PtResponsePacket, PacketPriorityNormal, responseFrameData)
}
