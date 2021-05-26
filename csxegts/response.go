package csxegts

import "github.com/kuznetsovin/egts-protocol/app/egts"

type ResponsePacketStatus struct {
	ResponsePacketID      uint16
	ProcessingResult      uint8
	ConfirmedRecordNumber uint16
	RecordStatus          uint8
}

func newResponseData(confirmedRecordNumber uint16, status uint8) *egts.SrResponse {
	data := egts.SrResponse{
		ConfirmedRecordNumber: confirmedRecordNumber,
		RecordStatus:          status,
	}

	return &data
}

func newResponseServiceFrameData(responsePacketID, confirmedRecordNumber uint16, resultCode, status uint8, serviceType byte) *egts.PtResponse {
	record := newRecord(egts.SrRecordResponseType, newResponseData(confirmedRecordNumber, status))
	subrecords := egts.RecordDataSet{*record}

	sdr := newServiceDataRecord(nil, RpPriorityNormal, serviceType, subrecords)
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

func ParseResponsePacket(packet *Packet) *ResponsePacketStatus {
	res := ResponsePacketStatus{}

	sfd := packet.ServicesFrameData.(*egts.PtResponse)
	res.ResponsePacketID = sfd.ResponsePacketID
	res.ProcessingResult = sfd.ProcessingResult
	for _, serviceRec := range *sfd.SDR.(*egts.ServiceDataSet) {
		for _, rec := range serviceRec.RecordDataSet {
			subrec := rec.SubrecordData.(*egts.SrResponse)
			res.ConfirmedRecordNumber = subrec.ConfirmedRecordNumber
			res.RecordStatus = subrec.RecordStatus
			break
		}
		break
	}

	return &res
}
