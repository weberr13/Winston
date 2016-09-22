package lfdb

import (
	"time"
)

//Get all the days between start and end where start is before end.
func Days(start, end time.Time) (times []time.Time) {
	if start.After(end) {
		return times
	}
	for {
		nt := start.Add(24 * time.Hour)
		start = nt
		if nt.Before(end) || nt.Equal(end) {
			times = append(times, nt)
		} else {
			break
		}
	}
	return times
}
