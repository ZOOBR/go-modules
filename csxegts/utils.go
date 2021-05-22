package csxegts

import (
	"sync/atomic"
	"time"
)

const uint16Max = 65_535

// ---------------------------------------------------------------------------------
// Counter
// ---------------------------------------------------------------------------------

// Counter is a struct for generating IDs for EGTS packets
type Counter struct {
	accumulator uint32
}

// Next returns next value of counter
//
// Based on getNextPid() & getNextRN() from github.com/kuznetsovin/egts-protocol
func (counter *Counter) Next() uint16 {
	if counter.accumulator < uint16Max {
		atomic.AddUint32(&counter.accumulator, 1)
	} else {
		counter.accumulator = 0
	}

	return uint16(atomic.LoadUint32(&counter.accumulator))
}

// ---------------------------------------------------------------------------------
// EGTS Time
// ---------------------------------------------------------------------------------

func EgtsTimeNowSeconds() uint32 {
	startDate := time.Date(2010, time.January, 1, 0, 0, 0, 0, time.UTC)
	return uint32(time.Now().Sub(startDate).Seconds())
}
