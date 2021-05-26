package csxegts

import (
	"time"

	"github.com/kuznetsovin/egts-protocol/app/egts"

	"gitlab.com/battler/modules/csxbinary"
	"gitlab.com/battler/modules/csxtelemetry"
)

func newPosData(pos *csxtelemetry.FlatPosition) *egts.SrPosData {
	data := egts.SrPosData{
		NavigationTime: time.Unix(int64(pos.Time/1000), 0),
		Latitude:       pos.P[1101],
		Longitude:      pos.P[1102],
		BB:             "0",
		CS:             CsWGS84,
		FIX:            Fix2D,
		VLD:            "1",
		Direction:      byte(pos.P[1104]),
		Odometer:       csxbinary.Float64ToByte(pos.P[1201]),
		Source:         SrcTimerEnabledIgnition,
	}
	data.DirectionHighestBit = data.Direction & 128

	if data.Latitude > 0 {
		data.LAHS = "0" // northern latitude
	} else {
		data.LAHS = "1" // south latitude
	}
	if data.Longitude > 0 {
		data.LOHS = "0" // eastern longitude
	} else {
		data.LOHS = "1" // west longitude
	}

	if pos.P[1103] != 0.0 {
		data.ALTE = "1"
		data.Altitude = csxbinary.Float64ToByte(pos.P[1103])

		if pos.P[1103] > 0.0 {
			data.AltitudeSign = 0
		} else {
			data.AltitudeSign = 1
		}
	} else {
		data.ALTE = "0"
	}

	if pos.P[1105] != 0.0 {
		data.MV = "1"
		data.Speed = uint16(pos.P[1105])
	} else {
		data.MV = "0"
	}

	return &data
}

func CreateTelemetryPacket(objectIdentifier uint32, pos *csxtelemetry.FlatPosition) (*Packet, uint16) {
	recordData := newPosData(pos)
	telemetryFrameData, recNum := newServiceFrameData(&objectIdentifier, RpPriorityNormal, egts.TeledataService, egts.SrPosDataType, recordData)

	return newPacket(PacketIDCounter.Next(), egts.PtAppdataPacket, PacketPriorityNormal, telemetryFrameData), recNum
}
