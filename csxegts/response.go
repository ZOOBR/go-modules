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
	var peerAddress, recipientAddress *uint16

	if sourcePacket.Route == "1" {
		peerAddress = &sourcePacket.PeerAddress
		recipientAddress = &sourcePacket.RecipientAddress
	}

	for _, rec := range *sourcePacket.ServicesFrameData.(*egts.ServiceDataSet) {
		serviceType = rec.SourceServiceType
		confirmedRecordNumber = rec.RecordNumber
		break
	}

	responseFrameData := newResponseServiceFrameData(responsePacketID, confirmedRecordNumber, resultCode, EgtsPcOk, serviceType)

	return newPacket(PacketIDCounter.Next(), egts.PtResponsePacket, PacketPriorityNormal, peerAddress, recipientAddress, responseFrameData)
}

func ParseResponsePacket(packet *Packet) (uint16, uint8, uint16, uint8) {
	var confirmedRecordNumber uint16
	var recordStatus uint8

	sfd := packet.ServicesFrameData.(*egts.PtResponse)
	responsePacketID := sfd.ResponsePacketID
	processingResult := sfd.ProcessingResult
	for _, serviceRec := range *sfd.SDR.(*egts.ServiceDataSet) {
		for _, rec := range serviceRec.RecordDataSet {
			subrec := rec.SubrecordData.(*egts.SrResponse)
			confirmedRecordNumber = subrec.ConfirmedRecordNumber
			recordStatus = subrec.RecordStatus
			break
		}
		break
	}

	return responsePacketID, processingResult, confirmedRecordNumber, recordStatus
}
