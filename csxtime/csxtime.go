package csxtime

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

// DayBeginWithTimezoneOffset return time begin of day with time zone offset
func DayBeginWithTimezoneOffset(timeCheck time.Time, offset int) time.Time {
	timeOffset := time.Duration(offset) * time.Second * 60 * 60
	dayBegin := timeCheck.Add(timeOffset)
	dayBegin = DayBegin(dayBegin)
	dayBegin = dayBegin.Add(-timeOffset)
	return dayBegin
}

// DayBegin return time begin of day
func DayBegin(timeCheck time.Time) time.Time {
	year, month, day := timeCheck.Date()
	dayBegin := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	return dayBegin
}

// DayEnd return time end of day
func DayEnd(timeCheck time.Time) time.Time {
	year, month, day := timeCheck.Date()
	dayBegin := time.Date(year, month, day, 23, 59, 59, 1e9-1, time.UTC)
	return dayBegin
}

// DayBeginStr return time begin of day to string
func DayBeginStr(timeCheck time.Time) string {
	return DayBegin(timeCheck).Format("2006-01-02 15:04:05.000000")
}

// DayEndStr return time begin of day to string
func DayEndStr(timeCheck time.Time) string {
	return DayEnd(timeCheck).Format("2006-01-02 15:04:05.000000")
}

func NowTimeToMilliseconds() float64 {
	return float64(time.Now().UTC().UnixNano() / int64(time.Millisecond))
}
