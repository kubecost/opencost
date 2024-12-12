package timeutil

import (
	"testing"
	"time"
)

func Test_DurationString(t *testing.T) {
	testCases := map[string]struct {
		duration         time.Duration
		expectedDuration string
	}{
		"1a": {
			duration:         0,
			expectedDuration: "",
		},
		"1b": {
			duration:         24 * time.Hour,
			expectedDuration: "1d",
		},
		"1c": {
			duration:         24*time.Hour + 5*time.Minute,
			expectedDuration: "1445m",
		},
		"1d": {
			duration:         25 * time.Hour,
			expectedDuration: "25h",
		},
		"1e": {
			duration:         25 * time.Hour,
			expectedDuration: "25h",
		},
		"1f": {
			duration:         72 * time.Hour,
			expectedDuration: "3d",
		},
		"1g": {
			duration:         25 * time.Hour,
			expectedDuration: "25h",
		},
		"1h": {
			duration:         24*time.Hour + time.Second,
			expectedDuration: "86401s",
		},
		// Expect empty strings if durations are negative
		"1i": {
			duration:         -25 * time.Hour,
			expectedDuration: "",
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			dur := DurationString(test.duration)
			if dur != test.expectedDuration {
				t.Fatalf("DurationOffsetStrings: exp (%s); act (%s)", test.expectedDuration, dur)
			}
		})
	}
}

func Test_DurationToPromOffsetString(t *testing.T) {
	testCases := map[string]struct {
		duration         time.Duration
		expectedDuration string
	}{
		"1a": {
			duration:         0,
			expectedDuration: "",
		},
		"1b": {
			duration:         24 * time.Hour,
			expectedDuration: "offset 1d",
		},
		"1c": {
			duration:         24*time.Hour + 5*time.Minute,
			expectedDuration: "offset 1445m",
		},
		"1d": {
			duration:         25 * time.Hour,
			expectedDuration: "offset 25h",
		},
		"1e": {
			duration:         25 * time.Hour,
			expectedDuration: "offset 25h",
		},
		"1f": {
			duration:         72 * time.Hour,
			expectedDuration: "offset 3d",
		},
		"1g": {
			duration:         25 * time.Hour,
			expectedDuration: "offset 25h",
		},
		"1h": {
			duration:         24*time.Hour + time.Second,
			expectedDuration: "offset 86401s",
		},
		// Expect empty strings if durations are negative
		"1i": {
			duration:         -25 * time.Hour,
			expectedDuration: "",
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			dur := DurationToPromOffsetString(test.duration)
			if dur != test.expectedDuration {
				t.Fatalf("DurationOffsetStrings: exp (%s); act (%s)", test.expectedDuration, dur)
			}
		})
	}
}

func Test_FormatStoreResolution(t *testing.T) {
	testCases := map[string]struct {
		duration         time.Duration
		expectedDuration string
	}{
		"1a": {
			duration:         0,
			expectedDuration: "0s",
		},
		"1b": {
			duration:         24 * time.Hour,
			expectedDuration: "1d",
		},
		"1c": {
			duration:         24*time.Hour + 5*time.Minute,
			expectedDuration: "1d",
		},
		"1d": {
			duration:         25 * time.Hour,
			expectedDuration: "1d",
		},
		"1e": {
			duration:         25 * time.Hour,
			expectedDuration: "1d",
		},
		"1f": {
			duration:         72 * time.Hour,
			expectedDuration: "3d",
		},
		"1g": {
			duration:         25 * time.Hour,
			expectedDuration: "1d",
		},
		"1h": {
			duration:         24*time.Hour + time.Second,
			expectedDuration: "1d",
		},
		// Expect empty strings if durations are negative
		"1i": {
			duration:         -25 * time.Hour,
			expectedDuration: "-25h0m0s",
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			dur := FormatStoreResolution(test.duration)
			if dur != test.expectedDuration {
				t.Fatalf("DurationOffsetStrings: exp (%s); act (%s)", test.expectedDuration, dur)
			}
		})
	}
}

func Test_DurationOffsetStrings(t *testing.T) {
	testCases := map[string]struct {
		duration         time.Duration
		offset           time.Duration
		expectedDuration string
		expectedOffset   string
	}{
		"1a": {
			duration:         0,
			offset:           0,
			expectedDuration: "",
			expectedOffset:   "",
		},
		"1b": {
			duration:         24 * time.Hour,
			offset:           0,
			expectedDuration: "1d",
			expectedOffset:   "",
		},
		"1c": {
			duration:         24*time.Hour + 5*time.Minute,
			offset:           0,
			expectedDuration: "1445m",
			expectedOffset:   "",
		},
		"1d": {
			duration:         25 * time.Hour,
			offset:           5 * time.Minute,
			expectedDuration: "25h",
			expectedOffset:   "5m",
		},
		"1e": {
			duration:         25 * time.Hour,
			offset:           60 * time.Minute,
			expectedDuration: "25h",
			expectedOffset:   "1h",
		},
		"1f": {
			duration:         72 * time.Hour,
			offset:           1440 * time.Minute,
			expectedDuration: "3d",
			expectedOffset:   "1d",
		},
		"1g": {
			duration:         25 * time.Hour,
			offset:           1 * time.Second,
			expectedDuration: "25h",
			expectedOffset:   "1s",
		},
		"1h": {
			duration:         24*time.Hour + time.Second,
			offset:           1 * time.Second,
			expectedDuration: "86401s",
			expectedOffset:   "1s",
		},
		// Expect empty strings if durations are negative
		"1i": {
			duration:         -25 * time.Hour,
			offset:           -1 * time.Second,
			expectedDuration: "",
			expectedOffset:   "",
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			dur, off := DurationOffsetStrings(test.duration, test.offset)
			if dur != test.expectedDuration || off != test.expectedOffset {
				t.Fatalf("DurationOffsetStrings: exp (%s %s); act (%s, %s)", test.expectedDuration, test.expectedOffset, dur, off)
			}
		})
	}
}

func Test_ParseDuration(t *testing.T) {
	testCases := map[string]struct {
		input    string
		expected time.Duration
	}{
		"expected": {
			input:    "3h",
			expected: time.Hour * 3,
		},
		"white space": {
			input:    " 4s ",
			expected: time.Second * 4,
		},
		"prom prefix": {
			input:    "offset 3m",
			expected: time.Minute * 3,
		},
		"prom prefix white space": {
			input:    " offset 3d ",
			expected: 24.0 * time.Hour * 3,
		},
		"ms duration": {
			input:    "100ms",
			expected: 100 * time.Millisecond,
		},
		"complex duration": {
			input:    "2d3h14m2s",
			expected: (24 * time.Hour * 2) + (3 * time.Hour) + (14 * time.Minute) + (2 * time.Second),
		},
		"negative duration": {
			input:    "-2d",
			expected: -48 * time.Hour,
		},
		"zero": {
			input:    "0h",
			expected: time.Duration(0),
		},
		"empty": {
			input:    "",
			expected: time.Duration(0),
		},
		"bad string": {
			input:    "oqwd3dk5hk",
			expected: time.Duration(0),
		},
		"digit": {
			input:    "3",
			expected: time.Duration(0),
		},
		"unit": {
			input:    "h",
			expected: time.Duration(0),
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			dur, _ := ParseDuration(test.input)
			if dur != test.expected {
				t.Errorf("Expected duration %v did not match result %v", test.expected, dur)
			}
		})
	}
}

func Test_CleanDurationString(t *testing.T) {
	testCases := map[string]struct {
		input    string
		expected string
	}{
		"white space": {
			input:    " 1d ",
			expected: "1d",
		},
		"no change": {
			input:    "1d",
			expected: "1d",
		},
		"prefix": {
			input:    "offset 1d",
			expected: "1d",
		},
		"prefix white space": {
			input:    " offset 1d ",
			expected: "1d",
		},
		"empty": {
			input:    "",
			expected: "",
		},
		"random": {
			input:    "oqwd3dk5hk",
			expected: "oqwd3dk5hk",
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			res := CleanDurationString(test.input)
			if res != test.expected {
				t.Errorf("Expected output %s did not match result %s", test.expected, res)
			}
		})
	}
}

func Test_FormatDurationStringDaysToHours(t *testing.T) {
	testCases := map[string]struct {
		input    string
		expected string
	}{
		"1 day": {
			input:    "1d",
			expected: "24h",
		},
		"2 days": {
			input:    "1d",
			expected: "24h",
		},
		"500 days": {
			input:    "500d",
			expected: "12000h",
		},
		"1h": {
			input:    "1h",
			expected: "1h",
		},
		"empty": {
			input:    "",
			expected: "",
		},
		"no unit": {
			input:    "1",
			expected: "1",
		},
		"random": {
			input:    "oqwd3dk5hk",
			expected: "oqwd3dk5hk",
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			res, _ := FormatDurationStringDaysToHours(test.input)
			if res != test.expected {
				t.Errorf("Expected output %s did not match result %s", test.expected, res)
			}
		})
	}
}

func TestRoundToStartOfWeek(t *testing.T) {
	sunday := time.Date(2023, 03, 26, 12, 12, 12, 12, time.UTC)
	roundedFromSunday := RoundToStartOfWeek(sunday)
	if roundedFromSunday.Day() != 26 || roundedFromSunday.Weekday() != time.Sunday {
		t.Errorf("expected date to be rounded to the same sunday, got: %d, %s", roundedFromSunday.Day(), roundedFromSunday.Weekday().String())
	}

	tuesday := time.Date(2023, 03, 28, 12, 12, 12, 12, time.UTC)
	roundedFromTuesday := RoundToStartOfWeek(tuesday)
	if roundedFromTuesday.Day() != 26 || roundedFromTuesday.Weekday() != time.Sunday {
		t.Errorf("expected date to be rounded to the same sunday, got: %d, %s", roundedFromTuesday.Day(), roundedFromTuesday.Weekday().String())
	}
}

func TestRoundToStartOfFollowingWeek(t *testing.T) {
	sunday := time.Date(2023, 03, 26, 12, 12, 12, 12, time.UTC)
	roundedFromSunday := RoundToStartOfFollowingWeek(sunday)
	if roundedFromSunday.Month() != 4 || roundedFromSunday.Day() != 2 || roundedFromSunday.Weekday() != time.Sunday {
		t.Errorf("expected date to be rounded to the same sunday, got: %d, %s", roundedFromSunday.Day(), roundedFromSunday.Weekday().String())
	}

	tuesday := time.Date(2023, 03, 28, 12, 12, 12, 12, time.UTC)
	roundedFromTuesday := RoundToStartOfFollowingWeek(tuesday)
	if roundedFromTuesday.Month() != 4 || roundedFromTuesday.Day() != 2 || roundedFromTuesday.Weekday() != time.Sunday {
		t.Errorf("expected date to be rounded to the same sunday, got: %d, %s", roundedFromTuesday.Day(), roundedFromTuesday.Weekday().String())
	}
}
