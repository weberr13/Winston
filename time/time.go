package time

import(
"time"
)

const (
	millisPerSecond     = int64(time.Second / time.Millisecond)
	nanosPerMillisecond = int64(time.Millisecond / time.Nanosecond)
)

func MSToTime(msTime int64) time.Time {
	return time.Unix(msTime/millisPerSecond,
		(msTime%millisPerSecond)*nanosPerMillisecond).UTC()
}

//TimeRoundToDay rounds a time object to the current day
func TimeRoundToDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

