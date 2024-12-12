package timeutil

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// SecsPerMin expresses the amount of seconds in a minute
	SecsPerMin = 60.0

	// SecsPerHour expresses the amount of seconds in a minute
	SecsPerHour = 3600.0

	// SecsPerDay expressed the amount of seconds in a day
	SecsPerDay = 86400.0

	// MinsPerHour expresses the amount of minutes in an hour
	MinsPerHour = 60.0

	// MinsPerDay expresses the amount of minutes in a day
	MinsPerDay = 1440.0

	// HoursPerDay expresses the amount of hours in a day
	HoursPerDay = 24.0

	// HoursPerMonth expresses the amount of hours in a month
	HoursPerMonth = 730.0

	// DaysPerMonth expresses the amount of days in a month
	DaysPerMonth = 30.42

	// Day expresses 24 hours
	Day = time.Hour * 24.0

	Week = Day * 7.0
)

var utcOffsetRegex = regexp.MustCompile(`^(\+|-)(\d\d):(\d\d)$`)

// DurationString converts a duration to a Prometheus-compatible string in
// terms of days, hours, minutes, or seconds.
func DurationString(duration time.Duration) string {
	durSecs := int64(duration.Seconds())

	durStr := ""
	if durSecs > 0 {
		if durSecs%SecsPerDay == 0 {
			// convert to days
			durStr = fmt.Sprintf("%dd", durSecs/SecsPerDay)
		} else if durSecs%SecsPerHour == 0 {
			// convert to hours
			durStr = fmt.Sprintf("%dh", durSecs/SecsPerHour)
		} else if durSecs%SecsPerMin == 0 {
			// convert to mins
			durStr = fmt.Sprintf("%dm", durSecs/SecsPerMin)
		} else if durSecs > 0 {
			// default to secs, as long as duration is positive
			durStr = fmt.Sprintf("%ds", durSecs)
		}
	}

	return durStr
}

// DurationToPromOffsetString returns a Prometheus formatted string with leading offset or empty string if given a negative duration
func DurationToPromOffsetString(duration time.Duration) string {
	dirStr := DurationString(duration)
	if dirStr != "" {
		dirStr = fmt.Sprintf("offset %s", dirStr)
	}
	return dirStr
}

// DurationOffsetStrings converts a (duration, offset) pair to Prometheus-
// compatible strings in terms of days, hours, minutes, or seconds.
func DurationOffsetStrings(duration, offset time.Duration) (string, string) {
	return DurationString(duration), DurationString(offset)
}

// ParseUTCOffset attempts to parse a UTC offset string. Returns a valid time.Duration
// if error is nil.
func ParseUTCOffset(offsetStr string) (time.Duration, error) {
	offset := time.Duration(0)

	if offsetStr != "" {
		match := utcOffsetRegex.FindStringSubmatch(offsetStr)
		if match == nil {
			return offset, fmt.Errorf("Illegal UTC offset format: %s", offsetStr)
		}

		sig := 1
		if match[1] == "-" {
			sig = -1
		}

		hrs64, err := strconv.ParseInt(match[2], 10, 64)
		if err != nil {
			return offset, fmt.Errorf("Failed to parse UTC offset hours: %w", err)
		}
		hrs := sig * int(hrs64)

		mins64, err := strconv.ParseInt(match[3], 10, 64)
		if err != nil {
			return offset, fmt.Errorf("Failed to parse UTC offset mins: %w", err)
		}
		mins := sig * int(mins64)

		offset = time.Duration(hrs)*time.Hour + time.Duration(mins)
	}

	return offset, nil
}

// FormatStoreResolution provides a clean notation for ETL store resolutions.
// e.g. daily => 1d; hourly => 1h
func FormatStoreResolution(dur time.Duration) string {
	if dur >= 24*time.Hour {
		return fmt.Sprintf("%dd", int(dur.Hours()/24.0))
	} else if dur >= time.Hour {
		return fmt.Sprintf("%dh", int(dur.Hours()))
	}
	return fmt.Sprint(dur)
}

// ParseDuration parses a duration string.
// A duration string is a possibly signed sequence of
// decimal numbers, each with optional fraction and a unit suffix,
// such as "300ms", "-1.5h" or "2h45m".
// Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h", "d"
func ParseDuration(duration string) (time.Duration, error) {
	duration = CleanDurationString(duration)
	return goParseDuration(duration)
}

// unitMap contains a list of units that can be parsed by ParseDuration
var unitMap = map[string]int64{
	"ns": int64(time.Nanosecond),
	"us": int64(time.Microsecond),
	"µs": int64(time.Microsecond), // U+00B5 = micro symbol
	"μs": int64(time.Microsecond), // U+03BC = Greek letter mu
	"ms": int64(time.Millisecond),
	"s":  int64(time.Second),
	"m":  int64(time.Minute),
	"h":  int64(time.Hour),
	"d":  int64(Day),
	"w":  int64(Week),
}

// goParseDuration is time.ParseDuration lifted from the go std library and enhanced with the ability to
// handle the "d" (day) unit. The contents of the function itself are identical to the std library, it is
// only the unitMap above that contains the added unit.
func goParseDuration(s string) (time.Duration, error) {
	// [-+]?([0-9]*(\.[0-9]*)?[a-z]+)+
	orig := s
	var d int64
	neg := false

	// Consume [-+]?
	if s != "" {
		c := s[0]
		if c == '-' || c == '+' {
			neg = c == '-'
			s = s[1:]
		}
	}
	// Special case: if all that is left is "0", this is zero.
	if s == "0" {
		return 0, nil
	}
	if s == "" {
		return 0, errors.New("time: invalid duration " + quote(orig))
	}
	for s != "" {
		var (
			v, f  int64       // integers before, after decimal point
			scale float64 = 1 // value = v + f/scale
		)

		var err error

		// The next character must be [0-9.]
		if !(s[0] == '.' || '0' <= s[0] && s[0] <= '9') {
			return 0, errors.New("time: invalid duration " + quote(orig))
		}
		// Consume [0-9]*
		pl := len(s)
		v, s, err = leadingInt(s)
		if err != nil {
			return 0, errors.New("time: invalid duration " + quote(orig))
		}
		pre := pl != len(s) // whether we consumed anything before a period

		// Consume (\.[0-9]*)?
		post := false
		if s != "" && s[0] == '.' {
			s = s[1:]
			pl := len(s)
			f, scale, s = leadingFraction(s)
			post = pl != len(s)
		}
		if !pre && !post {
			// no digits (e.g. ".s" or "-.s")
			return 0, errors.New("time: invalid duration " + quote(orig))
		}

		// Consume unit.
		i := 0
		for ; i < len(s); i++ {
			c := s[i]
			if c == '.' || '0' <= c && c <= '9' {
				break
			}
		}
		if i == 0 {
			return 0, errors.New("time: missing unit in duration " + quote(orig))
		}
		u := s[:i]
		s = s[i:]
		unit, ok := unitMap[u]
		if !ok {
			return 0, errors.New("time: unknown unit " + quote(u) + " in duration " + quote(orig))
		}
		if v > (1<<63-1)/unit {
			// overflow
			return 0, errors.New("time: invalid duration " + quote(orig))
		}
		v *= unit
		if f > 0 {
			// float64 is needed to be nanosecond accurate for fractions of hours.
			// v >= 0 && (f*unit/scale) <= 3.6e+12 (ns/h, h is the largest unit)
			v += int64(float64(f) * (float64(unit) / scale))
			if v < 0 {
				// overflow
				return 0, errors.New("time: invalid duration " + quote(orig))
			}
		}
		d += v
		if d < 0 {
			// overflow
			return 0, errors.New("time: invalid duration " + quote(orig))
		}
	}

	if neg {
		d = -d
	}

	return time.Duration(d), nil
}

// CleanDurationString removes prometheus formatted prefix "offset " allong with leading a trailing whitespace
// from duration string, leaving behind a string with format [0-9+](s|m|d|h)
func CleanDurationString(duration string) string {
	duration = strings.TrimSpace(duration)
	duration = strings.TrimPrefix(duration, "offset ")
	return duration
}

// ParseTimeRange returns a start and end time, respectively, which are converted from
// a duration and offset, defined as strings with Prometheus-style syntax.
func ParseTimeRange(duration, offset time.Duration) (time.Time, time.Time) {
	// endTime defaults to the current time, unless an offset is explicitly declared,
	// in which case it shifts endTime back by given duration
	endTime := time.Now()
	if offset > 0 {
		endTime = endTime.Add(-1 * offset)
	}

	startTime := endTime.Add(-1 * duration)

	return startTime, endTime
}

// FormatDurationStringDaysToHours converts string from format [0-9+]d to [0-9+]h
func FormatDurationStringDaysToHours(param string) (string, error) {
	//check that input matches format
	ok, err := regexp.MatchString("[0-9+]d", param)
	if !ok {
		return param, fmt.Errorf("FormatDurationStringDaysToHours: input string (%s) not formatted as [0-9+]d", param)
	}
	if err != nil {
		return "", err
	}
	// convert days to hours
	if param[len(param)-1:] == "d" {
		count := param[:len(param)-1]
		val, err := strconv.ParseInt(count, 10, 64)
		if err != nil {
			return "", err
		}
		val = val * 24
		param = fmt.Sprintf("%dh", val)
	}

	return param, nil
}

// RoundToStartOfWeek creates a new time.Time for the preceding Sunday 00:00 UTC
func RoundToStartOfWeek(t time.Time) time.Time {
	date := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	daysFromSunday := int(date.Weekday())
	return date.Add(-1 * time.Duration(daysFromSunday) * Day)
}

// RoundToStartOfFollowingWeek creates a new time.Time for the following Sunday 00:00 UTC
func RoundToStartOfFollowingWeek(t time.Time) time.Time {
	date := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	daysFromSunday := 7 - int(date.Weekday())
	return date.Add(time.Duration(daysFromSunday) * Day)
}

// JobTicker is a ticker used to synchronize the next run of a repeating
// process. The designated use-case is for infinitely-looping selects,
// where a timeout or an exit channel might cancel the process, but otherwise
// the intent is to wait at the select for some amount of time until the
// next run. This differs from a standard ticker, which ticks without
// waiting and drops any missed ticks; rather, this ticker must be kicked
// off manually for each tick, so that after the current run of the job
// completes, the timer starts again.
type JobTicker struct {
	Ch     <-chan time.Time
	ch     chan time.Time
	closed bool
	mx     sync.Mutex
}

// NewJobTicker instantiates a new JobTicker.
func NewJobTicker() *JobTicker {
	c := make(chan time.Time)

	return &JobTicker{
		Ch:     c,
		ch:     c,
		closed: false,
	}
}

// Close closes the JobTicker channels
func (jt *JobTicker) Close() {
	jt.mx.Lock()
	defer jt.mx.Unlock()

	if jt.closed {
		return
	}

	jt.closed = true
	close(jt.ch)
}

// TickAt schedules the next tick of the ticker for the given time in the
// future. If the time is not in the future, the ticker will tick immediately.
func (jt *JobTicker) TickAt(t time.Time) {
	go func(t time.Time) {
		n := time.Now()
		if t.After(n) {
			time.Sleep(t.Sub(n))
		}

		jt.mx.Lock()
		defer jt.mx.Unlock()

		if !jt.closed {
			jt.ch <- time.Now()
		}
	}(t)
}

// TickIn schedules the next tick of the ticker for the given duration into
// the future. If the duration is less than or equal to zero, the ticker will
// tick immediately.
func (jt *JobTicker) TickIn(d time.Duration) {
	go func(d time.Duration) {
		if d > 0 {
			time.Sleep(d)
		}

		jt.mx.Lock()
		defer jt.mx.Unlock()

		if !jt.closed {
			jt.ch <- time.Now()
		}
	}(d)
}

// NOTE: The following functions were lifted from the go std library to support the ParseDuration enhancement
// NOTE: described above.

const (
	lowerhex  = "0123456789abcdef"
	runeSelf  = 0x80
	runeError = '\uFFFD'
)

// quote is lifted from the go std library to support the custom ParseDuration enhancement
func quote(s string) string {
	buf := make([]byte, 1, len(s)+2) // slice will be at least len(s) + quotes
	buf[0] = '"'
	for i, c := range s {
		if c >= runeSelf || c < ' ' {
			// This means you are asking us to parse a time.Duration or
			// time.Location with unprintable or non-ASCII characters in it.
			// We don't expect to hit this case very often. We could try to
			// reproduce strconv.Quote's behavior with full fidelity but
			// given how rarely we expect to hit these edge cases, speed and
			// conciseness are better.
			var width int
			if c == runeError {
				width = 1
				if i+2 < len(s) && s[i:i+3] == string(runeError) {
					width = 3
				}
			} else {
				width = len(string(c))
			}
			for j := 0; j < width; j++ {
				buf = append(buf, `\x`...)
				buf = append(buf, lowerhex[s[i+j]>>4])
				buf = append(buf, lowerhex[s[i+j]&0xF])
			}
		} else {
			if c == '"' || c == '\\' {
				buf = append(buf, '\\')
			}
			buf = append(buf, string(c)...)
		}
	}
	buf = append(buf, '"')
	return string(buf)
}

// leadingFraction consumes the leading [0-9]* from s.
// It is used only for fractions, so does not return an error on overflow,
// it just stops accumulating precision.
func leadingFraction(s string) (x int64, scale float64, rem string) {
	i := 0
	scale = 1
	overflow := false
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		if overflow {
			continue
		}
		if x > (1<<63-1)/10 {
			// It's possible for overflow to give a positive number, so take care.
			overflow = true
			continue
		}
		y := x*10 + int64(c) - '0'
		if y < 0 {
			overflow = true
			continue
		}
		x = y
		scale *= 10
	}
	return x, scale, s[i:]
}

var errLeadingInt = errors.New("time: bad [0-9]*") // never printed

// leadingInt consumes the leading [0-9]* from s.
func leadingInt(s string) (x int64, rem string, err error) {
	i := 0
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		if x > (1<<63-1)/10 {
			// overflow
			return 0, "", errLeadingInt
		}
		x = x*10 + int64(c) - '0'
		if x < 0 {
			// overflow
			return 0, "", errLeadingInt
		}
	}
	return x, s[i:], nil
}

// EarlierOf returns the second time passed in if both are equal
func EarlierOf(timeOne, timeTwo time.Time) time.Time {
	if timeOne.Before(timeTwo) {
		return timeOne
	}
	return timeTwo
}

// LaterOf returns the second time passed in if both are equal
func LaterOf(timeOne, timeTwo time.Time) time.Time {
	if timeOne.After(timeTwo) {
		return timeOne
	}
	return timeTwo
}
