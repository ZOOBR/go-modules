package csxTime

import "time"

// MonthInterval return begin and end time of month by setted time
func MonthInterval(timeCheck time.Time) (firstDay, lastDay time.Time) {
	year, month, _ := timeCheck.Date()
	firstDay = time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	lastDay = time.Date(year, month+1, 1, 0, 0, 0, -1, time.UTC)
	return firstDay, lastDay
}

// MonthEnd return time end of month
func MonthEnd(timeCheck time.Time) time.Time {
	year, month, _ := timeCheck.Date()
	lastDay := time.Date(year, month+1, 1, 0, 0, 0, -1, time.UTC)
	return lastDay
}

// MonthBegin return time begin of month
func MonthBegin(timeCheck time.Time) time.Time {
	year, month, _ := timeCheck.Date()
	firstDay := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	return firstDay
}
