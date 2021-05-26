package csxegts

import "github.com/kuznetsovin/egts-protocol/app/egts"

func ParseResultPacket(packet *Packet) uint8 {
	var resultCode uint8

	for _, serviceRec := range *packet.ServicesFrameData.(*egts.ServiceDataSet) {
		for _, rec := range serviceRec.RecordDataSet {
			subrec := rec.SubrecordData.(*egts.SrResultCode)
			resultCode = subrec.ResultCode
			break
		}
		break
	}

	return resultCode
}
