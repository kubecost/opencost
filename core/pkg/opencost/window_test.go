package opencost

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/opencost/opencost/core/pkg/util/timeutil"

	"github.com/opencost/opencost/core/pkg/env"
)

const (
	ThanosEnabledEnvVarName = "THANOS_ENABLED"
	UtcOffsetEnvVarName     = "UTC_OFFSET"
)

func TestRoundBack(t *testing.T) {
	boulder := time.FixedZone("Boulder", -7*60*60)
	beijing := time.FixedZone("Beijing", 8*60*60)

	to := time.Date(2020, time.January, 1, 0, 0, 0, 0, boulder)
	tb := RoundBack(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 1, 0, 0, 0, 0, boulder)) {
		t.Fatalf("RoundBack: expected 2020-01-01T00:00:00-07:00; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 0, 0, 1, 0, boulder)
	tb = RoundBack(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 1, 0, 0, 0, 0, boulder)) {
		t.Fatalf("RoundBack: expected 2020-01-01T00:00:00-07:00; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 12, 37, 48, 0, boulder)
	tb = RoundBack(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 1, 0, 0, 0, 0, boulder)) {
		t.Fatalf("RoundBack: expected 2020-01-01T00:00:00-07:00; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 23, 37, 48, 0, boulder)
	tb = RoundBack(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 1, 0, 0, 0, 0, boulder)) {
		t.Fatalf("RoundBack: expected 2020-01-01T00:00:00-07:00; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 0, 0, 0, 0, beijing)
	tb = RoundBack(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 1, 0, 0, 0, 0, beijing)) {
		t.Fatalf("RoundBack: expected 2020-01-01T00:00:00+08:00; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 0, 0, 1, 0, beijing)
	tb = RoundBack(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 1, 0, 0, 0, 0, beijing)) {
		t.Fatalf("RoundBack: expected 2020-01-01T00:00:00+08:00; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 12, 37, 48, 0, beijing)
	tb = RoundBack(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 1, 0, 0, 0, 0, beijing)) {
		t.Fatalf("RoundBack: expected 2020-01-01T00:00:00+08:00; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 23, 59, 59, 0, beijing)
	tb = RoundBack(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 1, 0, 0, 0, 0, beijing)) {
		t.Fatalf("RoundBack: expected 2020-01-01T00:00:00+08:00; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	tb = RoundBack(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("RoundBack: expected 2020-01-01T00:00:00Z; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 0, 0, 1, 0, time.UTC)
	tb = RoundBack(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("RoundBack: expected 2020-01-01T00:00:00Z; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 12, 37, 48, 0, time.UTC)
	tb = RoundBack(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("RoundBack: expected 2020-01-01T00:00:00Z; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 23, 59, 0, 0, time.UTC)
	tb = RoundBack(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("RoundBack: expected 2020-01-01T00:00:00Z; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 23, 59, 0, 0, time.UTC)
	tb = RoundBack(to, timeutil.Week)
	if !tb.Equal(time.Date(2019, time.December, 29, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("RoundForward: expected 2019-12-29T00:00:00Z; actual %s", tb)
	}
}

func TestRoundForward(t *testing.T) {
	boulder := time.FixedZone("Boulder", -7*60*60)
	beijing := time.FixedZone("Beijing", 8*60*60)

	to := time.Date(2020, time.January, 1, 0, 0, 0, 0, boulder)
	tb := RoundForward(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 1, 0, 0, 0, 0, boulder)) {
		t.Fatalf("RoundForward: expected 2020-01-01T00:00:00-07:00; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 0, 0, 1, 0, boulder)
	tb = RoundForward(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 2, 0, 0, 0, 0, boulder)) {
		t.Fatalf("RoundForward: expected 2020-01-02T00:00:00-07:00; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 12, 37, 48, 0, boulder)
	tb = RoundForward(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 2, 0, 0, 0, 0, boulder)) {
		t.Fatalf("RoundForward: expected 2020-01-02T00:00:00-07:00; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 23, 37, 48, 0, boulder)
	tb = RoundForward(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 2, 0, 0, 0, 0, boulder)) {
		t.Fatalf("RoundForward: expected 2020-01-02T00:00:00-07:00; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 0, 0, 0, 0, beijing)
	tb = RoundForward(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 1, 0, 0, 0, 0, beijing)) {
		t.Fatalf("RoundForward: expected 2020-01-01T00:00:00+08:00; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 0, 0, 1, 0, beijing)
	tb = RoundForward(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 2, 0, 0, 0, 0, beijing)) {
		t.Fatalf("RoundForward: expected 2020-01-02T00:00:00+08:00; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 12, 37, 48, 0, beijing)
	tb = RoundForward(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 2, 0, 0, 0, 0, beijing)) {
		t.Fatalf("RoundForward: expected 2020-01-02T00:00:00+08:00; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 23, 59, 59, 0, beijing)
	tb = RoundForward(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 2, 0, 0, 0, 0, beijing)) {
		t.Fatalf("RoundForward: expected 2020-01-02T00:00:00+08:00; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	tb = RoundForward(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("RoundForward: expected 2020-01-01T00:00:00Z; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 0, 0, 1, 0, time.UTC)
	tb = RoundForward(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 2, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("RoundForward: expected 2020-01-02T00:00:00Z; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 12, 37, 48, 0, time.UTC)
	tb = RoundForward(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 2, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("RoundForward: expected 2020-01-02T00:00:00Z; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 23, 59, 0, 0, time.UTC)
	tb = RoundForward(to, 24*time.Hour)
	if !tb.Equal(time.Date(2020, time.January, 2, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("RoundForward: expected 2020-01-02T00:00:00Z; actual %s", tb)
	}

	to = time.Date(2020, time.January, 1, 23, 59, 0, 0, time.UTC)
	tb = RoundForward(to, timeutil.Week)
	if !tb.Equal(time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("RoundForward: expected 2020-01-05T00:00:00Z; actual %s", tb)
	}

	to = time.Date(2020, time.January, 5, 23, 59, 0, 0, time.UTC)
	tb = RoundForward(to, timeutil.Week)
	if !tb.Equal(time.Date(2020, time.January, 12, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("RoundForward: expected 2020-01-05T00:00:00Z; actual %s", tb)
	}

	to = time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC)
	tb = RoundForward(to, timeutil.Week)
	if !tb.Equal(time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("RoundForward: expected 2020-01-05T00:00:00Z; actual %s", tb)
	}
}

func TestParseWindowUTC(t *testing.T) {
	now := time.Now().UTC()

	// "today" should span Now() and not produce an error
	today, err := ParseWindowUTC("today")
	if err != nil {
		t.Fatalf(`unexpected error parsing "today": %s`, err)
	}
	if today.Duration().Hours() != 24 {
		t.Fatalf(`expect: window "today" to have duration 24 hour; actual: %f hours`, today.Duration().Hours())
	}
	if !today.Contains(time.Now().UTC()) {
		t.Fatalf(`expect: window "today" to contain now; actual: %s`, today)
	}

	// "yesterday" should span Now() and not produce an error
	yesterday, err := ParseWindowUTC("yesterday")
	if err != nil {
		t.Fatalf(`unexpected error parsing "yesterday": %s`, err)
	}
	if yesterday.Duration().Hours() != 24 {
		t.Fatalf(`expect: window "yesterday" to have duration 24 hour; actual: %f hours`, yesterday.Duration().Hours())
	}
	if !yesterday.End().Before(time.Now().UTC()) {
		t.Fatalf(`expect: window "yesterday" to end before now; actual: %s ends after %s`, yesterday, time.Now().UTC())
	}

	week, err := ParseWindowUTC("week")
	hoursThisWeek := float64(time.Now().UTC().Weekday()) * 24.0
	if err != nil {
		t.Fatalf(`unexpected error parsing "week": %s`, err)
	}
	if week.Duration().Hours() < hoursThisWeek {
		t.Fatalf(`expect: window "week" to have at least %f hours; actual: %f hours`, hoursThisWeek, week.Duration().Hours())
	}
	if week.End().After(time.Now().UTC()) {
		t.Fatalf(`expect: window "week" to end before now; actual: %s ends after %s`, week, time.Now().UTC())
	}

	month, err := ParseWindowUTC("month")
	hoursThisMonth := float64(time.Now().UTC().Day()) * 24.0
	if err != nil {
		t.Fatalf(`unexpected error parsing "month": %s`, err)
	}
	if month.Duration().Hours() > hoursThisMonth || month.Duration().Hours() < (hoursThisMonth-24.0) {
		t.Fatalf(`expect: window "month" to have approximately %f hours; actual: %f hours`, hoursThisMonth, month.Duration().Hours())
	}

	// this test fails periodically if execution is so fast that time.Now() during the condition
	// check is the same as the end of the current month time computed by ParseWindowUTC
	// so we add one nanosecond to sure time.Now() is later than when invoked earlier
	if !month.End().Before(time.Now().UTC().Add(time.Nanosecond)) {
		t.Fatalf(`expect: window "month" to end before now; actual: %s ends after %s`, month, time.Now().UTC())
	}

	// TODO lastweek

	lastmonth, err := ParseWindowUTC("lastmonth")
	monthMinHours := float64(24 * 28)
	monthMaxHours := float64(24 * 31)
	firstOfMonth := now.Truncate(time.Hour * 24).Add(-24 * time.Hour * time.Duration(now.Day()-1))
	if err != nil {
		t.Fatalf(`unexpected error parsing "lastmonth": %s`, err)
	}
	if lastmonth.Duration().Hours() > monthMaxHours || lastmonth.Duration().Hours() < monthMinHours {
		t.Fatalf(`expect: window "lastmonth" to have approximately %f hours; actual: %f hours`, hoursThisMonth, lastmonth.Duration().Hours())
	}
	if !lastmonth.End().Equal(firstOfMonth) {
		t.Fatalf(`expect: window "lastmonth" to end on the first of the current month; actual: %s doesn't end on %s`, lastmonth, firstOfMonth)
	}

	ago12h := time.Now().UTC().Add(-12 * time.Hour)
	ago24h := time.Now().UTC().Add(-24 * time.Hour)
	ago36h := time.Now().UTC().Add(-36 * time.Hour)
	ago60h := time.Now().UTC().Add(-60 * time.Hour)

	// "24h" should have 24 hour duration and not produce an error
	dur24h, err := ParseWindowUTC("24h")
	if err != nil {
		t.Fatalf(`unexpected error parsing "24h": %s`, err)
	}
	if dur24h.Duration().Hours() != 24 {
		t.Fatalf(`expect: window "24h" to have duration 24 hour; actual: %f hours`, dur24h.Duration().Hours())
	}
	if !dur24h.Contains(ago12h) {
		t.Fatalf(`expect: window "24h" to contain 12 hours ago; actual: %s doesn't contain %s`, dur24h, ago12h)
	}
	if dur24h.Contains(ago36h) {
		t.Fatalf(`expect: window "24h" to not contain 36 hours ago; actual: %s contains %s`, dur24h, ago36h)
	}

	// "2d" should have 2 day duration and not produce an error
	dur2d, err := ParseWindowUTC("2d")
	if err != nil {
		t.Fatalf(`unexpected error parsing "2d": %s`, err)
	}
	if dur2d.Duration().Hours() != 48 {
		t.Fatalf(`expect: window "2d" to have duration 48 hour; actual: %f hours`, dur2d.Duration().Hours())
	}
	if !dur2d.Contains(ago24h) {
		t.Fatalf(`expect: window "2d" to contain 24 hours ago; actual: %s doesn't contain %s`, dur2d, ago24h)
	}
	if dur2d.Contains(ago60h) {
		t.Fatalf(`expect: window "2d" to not contain 60 hours ago; actual: %s contains %s`, dur2d, ago60h)
	}

	// "24h offset 14h" should have 24 hour duration and not produce an error
	dur24hOff14h, err := ParseWindowUTC("24h offset 14h")
	if err != nil {
		t.Fatalf(`unexpected error parsing "24h offset 14h": %s`, err)
	}
	if dur24hOff14h.Duration().Hours() != 24 {
		t.Fatalf(`expect: window "24h offset 14h" to have duration 24 hour; actual: %f hours`, dur24hOff14h.Duration().Hours())
	}
	if dur24hOff14h.Contains(ago12h) {
		t.Fatalf(`expect: window "24h offset 14h" not to contain 12 hours ago; actual: %s contains %s`, dur24hOff14h, ago12h)
	}
	if !dur24hOff14h.Contains(ago36h) {
		t.Fatalf(`expect: window "24h offset 14h" to contain 36 hours ago; actual: %s does not contain %s`, dur24hOff14h, ago36h)
	}

	april152020, _ := time.Parse(time.RFC3339, "2020-04-15T00:00:00Z")
	april102020, _ := time.Parse(time.RFC3339, "2020-04-10T00:00:00Z")
	april052020, _ := time.Parse(time.RFC3339, "2020-04-05T00:00:00Z")

	// "2020-04-08T00:00:00Z,2020-04-12T00:00:00Z" should have 96 hour duration and not produce an error
	april8to12, err := ParseWindowUTC("2020-04-08T00:00:00Z,2020-04-12T00:00:00Z")
	if err != nil {
		t.Fatalf(`unexpected error parsing "2020-04-08T00:00:00Z,2020-04-12T00:00:00Z": %s`, err)
	}
	if april8to12.Duration().Hours() != 96 {
		t.Fatalf(`expect: window %s to have duration 96 hour; actual: %f hours`, april8to12, april8to12.Duration().Hours())
	}
	if !april8to12.Contains(april102020) {
		t.Fatalf(`expect: window April 8-12 to contain April 10; actual: %s doesn't contain %s`, april8to12, april102020)
	}
	if april8to12.Contains(april052020) {
		t.Fatalf(`expect: window April 8-12 to not contain April 5; actual: %s contains %s`, april8to12, april052020)
	}
	if april8to12.Contains(april152020) {
		t.Fatalf(`expect: window April 8-12 to not contain April 15; actual: %s contains %s`, april8to12, april152020)
	}

	march152020, _ := time.Parse(time.RFC3339, "2020-03-15T00:00:00Z")
	march102020, _ := time.Parse(time.RFC3339, "2020-03-10T00:00:00Z")
	march052020, _ := time.Parse(time.RFC3339, "2020-03-05T00:00:00Z")

	// "1583712000,1583884800" should have 48 hour duration and not produce an error
	march9to11, err := ParseWindowUTC("1583712000,1583884800")
	if err != nil {
		t.Fatalf(`unexpected error parsing "2020-04-08T00:00:00Z,2020-04-12T00:00:00Z": %s`, err)
	}
	if march9to11.Duration().Hours() != 48 {
		t.Fatalf(`expect: window %s to have duration 48 hour; actual: %f hours`, march9to11, march9to11.Duration().Hours())
	}
	if !march9to11.Contains(march102020) {
		t.Fatalf(`expect: window March 9-11 to contain March 10; actual: %s doesn't contain %s`, march9to11, march102020)
	}
	if march9to11.Contains(march052020) {
		t.Fatalf(`expect: window March 9-11 to not contain March 5; actual: %s contains %s`, march9to11, march052020)
	}
	if march9to11.Contains(march152020) {
		t.Fatalf(`expect: window March 9-11 to not contain March 15; actual: %s contains %s`, march9to11, march152020)
	}
}

func BenchmarkParseWindowUTC(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_, err := ParseWindowUTC("2020-04-08T00:00:00Z,2020-04-12T00:00:00Z")
		if err != nil {
			b.Fatalf("error running benchmark: %s", err.Error())
		}
	}
}

func TestParseWindowWithOffsetString(t *testing.T) {
	// ParseWindowWithOffsetString should equal ParseWindowUTC when location == "UTC"
	// for all window string formats

	todayUTC, err := ParseWindowUTC("today")
	if err != nil {
		t.Fatalf(`unexpected error parsing "today": %s`, err)
	}
	todayTZ, err := ParseWindowWithOffsetString("today", "UTC")
	if err != nil {
		t.Fatalf(`unexpected error parsing "today": %s`, err)
	}
	if !todayUTC.ApproximatelyEqual(todayTZ, time.Millisecond) {
		t.Fatalf(`expect: window "today" UTC to equal "today" with timezone "UTC"; actual: %s not equal %s`, todayUTC, todayTZ)
	}

	yesterdayUTC, err := ParseWindowUTC("yesterday")
	if err != nil {
		t.Fatalf(`unexpected error parsing "yesterday": %s`, err)
	}
	yesterdayTZ, err := ParseWindowWithOffsetString("yesterday", "UTC")
	if err != nil {
		t.Fatalf(`unexpected error parsing "yesterday": %s`, err)
	}
	if !yesterdayUTC.ApproximatelyEqual(yesterdayTZ, time.Millisecond) {
		t.Fatalf(`expect: window "yesterday" UTC to equal "yesterday" with timezone "UTC"; actual: %s not equal %s`, yesterdayUTC, yesterdayTZ)
	}

	weekUTC, err := ParseWindowUTC("week")
	if err != nil {
		t.Fatalf(`unexpected error parsing "week": %s`, err)
	}
	weekTZ, err := ParseWindowWithOffsetString("week", "UTC")
	if err != nil {
		t.Fatalf(`unexpected error parsing "week": %s`, err)
	}
	if !weekUTC.ApproximatelyEqual(weekTZ, time.Millisecond) {
		t.Fatalf(`expect: window "week" UTC to equal "week" with timezone "UTC"; actual: %s not equal %s`, weekUTC, weekTZ)
	}

	monthUTC, err := ParseWindowUTC("month")
	if err != nil {
		t.Fatalf(`unexpected error parsing "month": %s`, err)
	}
	monthTZ, err := ParseWindowWithOffsetString("month", "UTC")
	if err != nil {
		t.Fatalf(`unexpected error parsing "month": %s`, err)
	}
	if !monthUTC.ApproximatelyEqual(monthTZ, time.Millisecond) {
		t.Fatalf(`expect: window "month" UTC to equal "month" with timezone "UTC"; actual: %s not equal %s`, monthUTC, monthTZ)
	}

	lastweekUTC, err := ParseWindowUTC("lastweek")
	if err != nil {
		t.Fatalf(`unexpected error parsing "lastweek": %s`, err)
	}
	lastweekTZ, err := ParseWindowWithOffsetString("lastweek", "UTC")
	if err != nil {
		t.Fatalf(`unexpected error parsing "lastweek": %s`, err)
	}
	if !lastweekUTC.ApproximatelyEqual(lastweekTZ, time.Millisecond) {
		t.Fatalf(`expect: window "lastweek" UTC to equal "lastweek" with timezone "UTC"; actual: %s not equal %s`, lastweekUTC, lastweekTZ)
	}

	lastmonthUTC, err := ParseWindowUTC("lastmonth")
	if err != nil {
		t.Fatalf(`unexpected error parsing "lastmonth": %s`, err)
	}
	lastmonthTZ, err := ParseWindowWithOffsetString("lastmonth", "UTC")
	if err != nil {
		t.Fatalf(`unexpected error parsing "lastmonth": %s`, err)
	}
	if !lastmonthUTC.ApproximatelyEqual(lastmonthTZ, time.Millisecond) {
		t.Fatalf(`expect: window "lastmonth" UTC to equal "lastmonth" with timezone "UTC"; actual: %s not equal %s`, lastmonthUTC, lastmonthTZ)
	}

	dur10mUTC, err := ParseWindowUTC("10m")
	if err != nil {
		t.Fatalf(`unexpected error parsing "10m": %s`, err)
	}
	dur10mTZ, err := ParseWindowWithOffsetString("10m", "UTC")
	if err != nil {
		t.Fatalf(`unexpected error parsing "10m": %s`, err)
	}
	if !dur10mUTC.ApproximatelyEqual(dur10mTZ, time.Millisecond) {
		t.Fatalf(`expect: window "10m" UTC to equal "10m" with timezone "UTC"; actual: %s not equal %s`, dur10mUTC, dur10mTZ)
	}

	dur24hUTC, err := ParseWindowUTC("24h")
	if err != nil {
		t.Fatalf(`unexpected error parsing "24h": %s`, err)
	}
	dur24hTZ, err := ParseWindowWithOffsetString("24h", "UTC")
	if err != nil {
		t.Fatalf(`unexpected error parsing "24h": %s`, err)
	}
	if !dur24hUTC.ApproximatelyEqual(dur24hTZ, time.Millisecond) {
		t.Fatalf(`expect: window "24h" UTC to equal "24h" with timezone "UTC"; actual: %s not equal %s`, dur24hUTC, dur24hTZ)
	}

	dur37dUTC, err := ParseWindowUTC("37d")
	if err != nil {
		t.Fatalf(`unexpected error parsing "37d": %s`, err)
	}
	dur37dTZ, err := ParseWindowWithOffsetString("37d", "UTC")
	if err != nil {
		t.Fatalf(`unexpected error parsing "37d": %s`, err)
	}
	if !dur37dUTC.ApproximatelyEqual(dur37dTZ, time.Millisecond) {
		t.Fatalf(`expect: window "37d" UTC to equal "37d" with timezone "UTC"; actual: %s not equal %s`, dur37dUTC, dur37dTZ)
	}

	// ParseWindowWithOffsetString should be the correct relative to ParseWindowUTC; i.e.
	// - for durations, the times should match, but the representations should differ
	//   by the number of hours offset
	// - for words like "today" and "yesterday", the times may not match, in which
	//   case, for example, "today" UTC-08:00 might equal "yesterday" UTC

	// fmtWindow only compares date and time to the minute, not second or
	// timezone. Helper for comparing timezone shifted windows.
	fmtWindow := func(w Window) string {
		s := "nil"
		if w.start != nil {
			s = w.start.Format("2006-01-02T15:04")
		}

		e := "nil"
		if w.end != nil {
			e = w.end.Format("2006-01-02T15:04")
		}
		return fmt.Sprintf("[%s, %s]", s, e)
	}

	// Test UTC-08:00 (California), UTC+03:00 (Moscow), UTC+12:00 (New Zealand), and UTC itself
	for _, offsetHrs := range []int{-8, 3, 12, 0} {
		offStr := fmt.Sprintf("+%02d:00", offsetHrs)
		if offsetHrs < 0 {
			offStr = fmt.Sprintf("-%02d:00", -offsetHrs)
		}
		off := time.Duration(offsetHrs) * time.Hour

		dur10mTZ, err = ParseWindowWithOffsetString("10m", offStr)
		if err != nil {
			t.Fatalf(`unexpected error parsing "10m": %s`, err)
		}
		if !dur10mTZ.ApproximatelyEqual(dur10mUTC, time.Second) {
			t.Fatalf(`expect: window "10m" UTC to equal "10m" with timezone "%s"; actual: %s not equal %s`, offStr, dur10mUTC, dur10mTZ)
		}
		if fmtWindow(dur10mTZ.Shift(-off)) != fmtWindow(dur10mUTC) {
			t.Fatalf(`expect: date, hour, and minute of window "10m" UTC to equal that of "10m" %s shifted by %s; actual: %s not equal %s`, offStr, off, fmtWindow(dur10mUTC), fmtWindow(dur10mTZ.Shift(-off)))
		}

		dur24hTZ, err = ParseWindowWithOffsetString("24h", offStr)
		if err != nil {
			t.Fatalf(`unexpected error parsing "24h": %s`, err)
		}
		if !dur24hTZ.ApproximatelyEqual(dur24hUTC, time.Second) {
			t.Fatalf(`expect: window "24h" UTC to equal "24h" with timezone "%s"; actual: %s not equal %s`, offStr, dur24hUTC, dur24hTZ)
		}
		if fmtWindow(dur24hTZ.Shift(-off)) != fmtWindow(dur24hUTC) {
			t.Fatalf(`expect: date, hour, and minute of window "24h" UTC to equal that of "24h" %s shifted by %s; actual: %s not equal %s`, offStr, off, fmtWindow(dur24hUTC), fmtWindow(dur24hTZ.Shift(-off)))
		}

		dur37dTZ, err = ParseWindowWithOffsetString("37d", offStr)
		if err != nil {
			t.Fatalf(`unexpected error parsing "37d": %s`, err)
		}
		if !dur37dTZ.ApproximatelyEqual(dur37dUTC, time.Second) {
			t.Fatalf(`expect: window "37d" UTC to equal "37d" with timezone "%s"; actual: %s not equal %s`, offStr, dur37dUTC, dur37dTZ)
		}
		if fmtWindow(dur37dTZ.Shift(-off)) != fmtWindow(dur37dUTC) {
			t.Fatalf(`expect: date, hour, and minute of window "37d" UTC to equal that of "37d" %s shifted by %s; actual: %s not equal %s`, offStr, off, fmtWindow(dur37dUTC), fmtWindow(dur37dTZ.Shift(-off)))
		}

		// "today" and "yesterday" should comply with the current day in each
		// respective timezone, depending on if it is ahead of, equal to, or
		// behind UTC at the given moment.

		todayTZ, err = ParseWindowWithOffsetString("today", offStr)
		if err != nil {
			t.Fatalf(`unexpected error parsing "today": %s`, err)
		}

		yesterdayTZ, err = ParseWindowWithOffsetString("yesterday", offStr)
		if err != nil {
			t.Fatalf(`unexpected error parsing "yesterday": %s`, err)
		}

		hoursSinceYesterdayUTC := time.Now().UTC().Sub(time.Now().UTC().Truncate(24.0 * time.Hour)).Hours()
		hoursUntilTomorrowUTC := 24.0 - hoursSinceYesterdayUTC
		aheadOfUTC := float64(offsetHrs)-hoursUntilTomorrowUTC > 0
		behindUTC := float64(offsetHrs)+hoursSinceYesterdayUTC < 0

		// yesterday in this timezone should equal today UTC
		if aheadOfUTC {
			if fmtWindow(yesterdayTZ) != fmtWindow(todayUTC) {
				t.Fatalf(`expect: window "today" UTC to equal "yesterday" with timezone "%s"; actual: %s not equal %s`, offStr, yesterdayTZ, todayUTC)
			}
		}

		// today in this timezone should equal yesterday UTC
		if behindUTC {
			if fmtWindow(todayTZ) != fmtWindow(yesterdayUTC) {
				t.Fatalf(`expect: window "today" UTC to equal "yesterday" with timezone "%s"; actual: %s not equal %s`, offStr, todayTZ, yesterdayUTC)
			}
		}

		// today in this timezone should equal today UTC, likewise for yesterday
		if !aheadOfUTC && !behindUTC {
			if fmtWindow(todayTZ) != fmtWindow(todayUTC) {
				t.Fatalf(`expect: window "today" UTC to equal "today" with timezone "%s"; actual: %s not equal %s`, offStr, todayTZ, todayUTC)
			}
			// yesterday in this timezone should equal yesterday UTC
			if fmtWindow(yesterdayTZ) != fmtWindow(yesterdayUTC) {
				t.Fatalf(`expect: window "yesterday" UTC to equal "yesterday" with timezone "%s"; actual: %s not equal %s`, offStr, yesterdayTZ, yesterdayUTC)
			}
		}
	}

}

func TestWindow_DurationOffsetStrings(t *testing.T) {
	w, err := ParseWindowUTC("1d")
	if err != nil {
		t.Fatalf(`unexpected error parsing "1d": %s`, err)
	}
	dur, off := w.DurationOffsetStrings()
	if dur != "1d" {
		t.Fatalf(`expect: window to be "1d"; actual: "%s"`, dur)
	}
	if off != "" {
		t.Fatalf(`expect: offset to be ""; actual: "%s"`, off)
	}

	w, err = ParseWindowUTC("3h")
	if err != nil {
		t.Fatalf(`unexpected error parsing "1d": %s`, err)
	}
	dur, off = w.DurationOffsetStrings()
	if dur != "3h" {
		t.Fatalf(`expect: window to be "3h"; actual: "%s"`, dur)
	}
	if off != "" {
		t.Fatalf(`expect: offset to be ""; actual: "%s"`, off)
	}

	w, err = ParseWindowUTC("10m")
	if err != nil {
		t.Fatalf(`unexpected error parsing "1d": %s`, err)
	}
	dur, off = w.DurationOffsetStrings()
	if dur != "10m" {
		t.Fatalf(`expect: window to be "10m"; actual: "%s"`, dur)
	}
	if off != "" {
		t.Fatalf(`expect: offset to be ""; actual: "%s"`, off)
	}

	w, err = ParseWindowUTC("1589448338,1589534798")
	if err != nil {
		t.Fatalf(`unexpected error parsing "1589448338,1589534798": %s`, err)
	}
	dur, off = w.DurationOffsetStrings()
	if dur != "1441m" {
		t.Fatalf(`expect: window to be "1441m"; actual: "%s"`, dur)
	}
	if off == "" {
		t.Fatalf(`expect: offset to not be ""; actual: "%s"`, off)
	}

	w, err = ParseWindowUTC("yesterday")
	if err != nil {
		t.Fatalf(`unexpected error parsing "1589448338,1589534798": %s`, err)
	}
	dur, _ = w.DurationOffsetStrings()
	if dur != "1d" {
		t.Fatalf(`expect: window to be "1d"; actual: "%s"`, dur)
	}
}

func TestParse_Window(t *testing.T) {
	now := time.Date(2024, time.May, 3, 8, 1, 4, 6, time.UTC)
	win, err := parseWindow("2h", now)
	if err != nil {
		t.Fatalf(`unexpected error parsing "2h": %s`, err)
	}

	expectedStart := time.Date(2024, time.May, 3, 7, 0, 0, 0, time.UTC)
	expectedEnd := time.Date(2024, time.May, 3, 9, 0, 0, 0, time.UTC)

	if !win.start.Equal(expectedStart) {
		t.Fatalf(`expect: window start to be %s; actual: %s`, expectedStart, win.start)
	}
	if !win.end.Equal(expectedEnd) {
		t.Fatalf(`expect: window end to be %s; actual: %s`, expectedEnd, win.end)
	}

	win, err = parseWindow("3d", now)
	if err != nil {
		t.Fatalf(`unexpected error parsing "3d": %s`, err)
	}

	expectedStart = time.Date(2024, time.May, 1, 0, 0, 0, 0, time.UTC)
	expectedEnd = time.Date(2024, time.May, 4, 0, 0, 0, 0, time.UTC)

	if !win.start.Equal(expectedStart) {
		t.Fatalf(`expect: window start to be %s; actual: %s`, expectedStart, win.start)
	}
	if !win.end.Equal(expectedEnd) {
		t.Fatalf(`expect: window end to be %s; actual: %s`, expectedEnd, win.end)
	}
}
func TestWindow_Duration(t *testing.T) {
	// Set-up and tear-down
	thanosEnabled := env.GetBool(ThanosEnabledEnvVarName, false)
	defer env.SetBool(ThanosEnabledEnvVarName, thanosEnabled)

	// Test for Prometheus (env.IsThanosEnabled() == false)
	env.SetBool(ThanosEnabledEnvVarName, false)
	if env.GetBool(ThanosEnabledEnvVarName, false) {
		t.Fatalf("expected env.IsThanosEnabled() == false")
	}

	now := time.Now().UTC()
	w, err := parseWindow("1d", now)
	if err != nil {
		t.Fatalf(`unexpected error parsing "1d": %s`, err)
	}
	if w.Duration() != 24*time.Hour {
		t.Fatalf(`expect: window to be 24 hours; actual: %s`, w.Duration())
	}

	w, err = ParseWindowUTC("2h")
	if err != nil {
		t.Fatalf(`unexpected error parsing "2h": %s`, err)
	}

	if w.Duration().String() != "2h0m0s" {
		t.Fatalf(`expect: window to be "2h"; actual: "%s"`, w.Duration().String())
	}
	w, err = ParseWindowUTC("10m")
	if err != nil {
		t.Fatalf(`unexpected error parsing "10m": %s`, err)
	}

	if w.Duration().String() != "10m0s" {
		t.Fatalf(`expect: window to be "10m"; actual: "%s"`, w.Duration().String())
	}

	w, err = ParseWindowUTC("1589448338,1589534798")
	if err != nil {
		t.Fatalf(`unexpected error parsing "1589448338,1589534798": %s`, err)
	}
	if w.Duration().String() != "24h1m0s" {
		t.Fatalf(`expect: window to be "24h1m0s"; actual: "%s"`, w.Duration().String())
	}

	w, err = ParseWindowUTC("yesterday")
	if err != nil {
		t.Fatalf(`unexpected error parsing "yesterday": %s`, err)
	}
	if w.Duration().String() != "24h0m0s" {
		t.Fatalf(`expect: window to be "24h0m0s"; actual: "%s"`, w.Duration().String())
	}

}

// TODO
// func TestWindow_Overlaps(t *testing.T) {}

// TODO
// func TestWindow_Contains(t *testing.T) {}

// TODO
// func TestWindow_Duration(t *testing.T) {}

// TODO
// func TestWindow_End(t *testing.T) {}

// TODO
// func TestWindow_Equal(t *testing.T) {}

// TODO
// func TestWindow_ExpandStart(t *testing.T) {}

// TODO
// func TestWindow_ExpandEnd(t *testing.T) {}

func TestWindow_Expand(t *testing.T) {

	t1 := time.Now().Round(time.Hour)
	t2 := t1.Add(34 * time.Minute)
	t3 := t1.Add(50 * time.Minute)
	t4 := t1.Add(84 * time.Minute)

	cases := []struct {
		windowToExpand Window
		windowArgument Window

		expected Window
	}{
		{
			windowToExpand: NewClosedWindow(t1, t2),
			windowArgument: NewClosedWindow(t3, t4),

			expected: NewClosedWindow(t1, t4),
		},
		{
			windowToExpand: NewClosedWindow(t3, t4),
			windowArgument: NewClosedWindow(t1, t2),

			expected: NewClosedWindow(t1, t4),
		},
		{
			windowToExpand: NewClosedWindow(t1, t3),
			windowArgument: NewClosedWindow(t2, t4),

			expected: NewClosedWindow(t1, t4),
		},
		{
			windowToExpand: NewClosedWindow(t2, t4),
			windowArgument: NewClosedWindow(t1, t3),

			expected: NewClosedWindow(t1, t4),
		},
		{
			windowToExpand: Window{},
			windowArgument: NewClosedWindow(t1, t2),

			expected: NewClosedWindow(t1, t2),
		},
		{
			windowToExpand: NewWindow(nil, &t2),
			windowArgument: NewWindow(nil, &t3),

			expected: NewWindow(nil, &t3),
		},
		{
			windowToExpand: NewWindow(&t2, nil),
			windowArgument: NewWindow(&t1, nil),

			expected: NewWindow(&t1, nil),
		},
	}

	for _, c := range cases {
		result := c.windowToExpand.Expand(c.windowArgument)
		if !result.Equal(c.expected) {
			t.Errorf("Expand %s with %s, expected %s but got %s", c.windowToExpand, c.windowArgument, c.expected, result)
		}
	}
}

// TODO
// func TestWindow_Start(t *testing.T) {}

// TODO
// func TestWindow_String(t *testing.T) {}

func TestWindow_GetPercentInWindow(t *testing.T) {
	dayStart := time.Date(2022, 12, 6, 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.Add(timeutil.Day)
	window := NewClosedWindow(dayStart, dayEnd)

	testcases := map[string]struct {
		window    Window
		itemStart time.Time
		itemEnd   time.Time
		expected  float64
	}{
		"matching start/matching end": {
			window:    window,
			itemStart: dayStart,
			itemEnd:   dayEnd,
			expected:  1.0,
		},
		"matching start/contained end": {
			window:    window,
			itemStart: dayStart,
			itemEnd:   dayEnd.Add(-time.Hour * 6),
			expected:  1.0,
		},
		"contained start/matching end": {
			window:    window,
			itemStart: dayStart.Add(time.Hour * 6),
			itemEnd:   dayEnd,
			expected:  1.0,
		},
		"contained start/contained end": {
			window:    window,
			itemStart: dayStart.Add(time.Hour * 6),
			itemEnd:   dayEnd.Add(-time.Hour * 6),
			expected:  1.0,
		},
		"before start/contained end": {
			window:    window,
			itemStart: dayStart.Add(-time.Hour * 12),
			itemEnd:   dayEnd.Add(-time.Hour * 12),
			expected:  0.5,
		},
		"before start/before end": {
			window:    window,
			itemStart: dayStart.Add(-time.Hour * 24),
			itemEnd:   dayEnd.Add(-time.Hour * 24),
			expected:  0.0,
		},
		"contained start/after end": {
			window:    window,
			itemStart: dayStart.Add(time.Hour * 12),
			itemEnd:   dayEnd.Add(time.Hour * 12),
			expected:  0.5,
		},
		"after start/after end": {
			window:    window,
			itemStart: dayStart.Add(time.Hour * 24),
			itemEnd:   dayEnd.Add(time.Hour * 24),
			expected:  0.0,
		},
		"before start/after end": {
			window:    window,
			itemStart: dayStart.Add(-time.Hour * 12),
			itemEnd:   dayEnd.Add(time.Hour * 12),
			expected:  0.5,
		},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			thatWindow := NewWindow(&tc.itemStart, &tc.itemEnd)
			if actual := tc.window.GetPercentInWindow(thatWindow); actual != tc.expected {
				t.Errorf("GetPercentInWindow() = %v, want %v", actual, tc.expected)
			}
		})
	}
}

func TestWindow_GetWindows(t *testing.T) {
	dayStart := time.Date(2022, 12, 6, 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.Add(timeutil.Day)
	loc, _ := time.LoadLocation("America/Vancouver")
	testCases := map[string]struct {
		start       time.Time
		end         time.Time
		windowSize  time.Duration
		expected    []Window
		expectedErr bool
	}{
		"mismatching tz": {
			start:       dayStart,
			end:         dayEnd.In(loc),
			windowSize:  time.Hour,
			expected:    nil,
			expectedErr: true,
		},
		"hour windows over 1 hours": {
			start:      dayStart,
			end:        dayStart.Add(time.Hour),
			windowSize: time.Hour,
			expected: []Window{
				NewClosedWindow(dayStart, dayStart.Add(time.Hour)),
			},
			expectedErr: false,
		},
		"hour windows over 3 hours": {
			start:      dayStart,
			end:        dayStart.Add(time.Hour * 3),
			windowSize: time.Hour,
			expected: []Window{
				NewClosedWindow(dayStart, dayStart.Add(time.Hour)),
				NewClosedWindow(dayStart.Add(time.Hour), dayStart.Add(time.Hour*2)),
				NewClosedWindow(dayStart.Add(time.Hour*2), dayStart.Add(time.Hour*3)),
			},
			expectedErr: false,
		},
		"hour windows off hour grid": {
			start:       dayStart.Add(time.Minute),
			end:         dayEnd.Add(time.Minute),
			windowSize:  time.Hour,
			expected:    nil,
			expectedErr: true,
		},
		"hour windows range not divisible by hour": {
			start:       dayStart,
			end:         dayStart.Add(time.Minute * 90),
			windowSize:  time.Hour,
			expected:    nil,
			expectedErr: true,
		},
		"day windows over 1 day": {
			start:      dayStart,
			end:        dayEnd,
			windowSize: timeutil.Day,
			expected: []Window{
				NewClosedWindow(dayStart, dayEnd),
			},
			expectedErr: false,
		},
		"day windows over 3 days": {
			start:      dayStart,
			end:        dayStart.Add(timeutil.Day * 3),
			windowSize: timeutil.Day,
			expected: []Window{
				NewClosedWindow(dayStart, dayStart.Add(timeutil.Day)),
				NewClosedWindow(dayStart.Add(timeutil.Day), dayStart.Add(timeutil.Day*2)),
				NewClosedWindow(dayStart.Add(timeutil.Day*2), dayStart.Add(timeutil.Day*3)),
			},
			expectedErr: false,
		},
		"day windows off day grid": {
			start:       dayStart.Add(time.Hour),
			end:         dayEnd.Add(time.Hour),
			windowSize:  timeutil.Day,
			expected:    nil,
			expectedErr: true,
		},
		"day windows range not divisible by day": {
			start:       dayStart,
			end:         dayEnd.Add(time.Hour),
			windowSize:  timeutil.Day,
			expected:    nil,
			expectedErr: true,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := GetWindows(tc.start, tc.end, tc.windowSize)
			if (err != nil) != tc.expectedErr {
				t.Errorf("GetWindows() error = %v, expectedErr %v", err, tc.expectedErr)
				return
			}
			if len(tc.expected) != len(actual) {
				t.Errorf("GetWindows() []window has incorrect length expected: %d, actual: %d", len(tc.expected), len(actual))
			}
			for i, actualWindow := range actual {
				expectedWindow := tc.expected[i]
				if !actualWindow.Equal(expectedWindow) {
					t.Errorf("GetWindow() window at index %d were not equal expected: %s, actual %s", i, expectedWindow.String(), actualWindow)
				}
			}
		})
	}
}

func TestWindow_GetWindowsForQueryWindow(t *testing.T) {
	dayStart := time.Date(2022, 12, 6, 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.Add(timeutil.Day)
	loc, _ := time.LoadLocation("America/Vancouver")
	testCases := map[string]struct {
		start       time.Time
		end         time.Time
		windowSize  time.Duration
		expected    []Window
		expectedErr bool
	}{
		"mismatching tz": {
			start:       dayStart,
			end:         dayEnd.In(loc),
			windowSize:  time.Hour,
			expected:    nil,
			expectedErr: true,
		},
		"hour windows over 1 hours": {
			start:      dayStart,
			end:        dayStart.Add(time.Hour),
			windowSize: time.Hour,
			expected: []Window{
				NewClosedWindow(dayStart, dayStart.Add(time.Hour)),
			},
			expectedErr: false,
		},
		"hour windows over 3 hours": {
			start:      dayStart,
			end:        dayStart.Add(time.Hour * 3),
			windowSize: time.Hour,
			expected: []Window{
				NewClosedWindow(dayStart, dayStart.Add(time.Hour)),
				NewClosedWindow(dayStart.Add(time.Hour), dayStart.Add(time.Hour*2)),
				NewClosedWindow(dayStart.Add(time.Hour*2), dayStart.Add(time.Hour*3)),
			},
			expectedErr: false,
		},
		"hour windows off hour grid": {
			start:      dayStart.Add(time.Minute),
			end:        dayStart.Add(time.Minute * 61),
			windowSize: time.Hour,
			expected: []Window{
				NewClosedWindow(dayStart.Add(time.Minute), dayStart.Add(time.Minute*61)),
			},
			expectedErr: false,
		},
		"hour windows range not divisible by hour": {
			start:      dayStart,
			end:        dayStart.Add(time.Minute * 90),
			windowSize: time.Hour,
			expected: []Window{
				NewClosedWindow(dayStart, dayStart.Add(time.Hour)),
				NewClosedWindow(dayStart.Add(time.Hour), dayStart.Add(time.Minute*90)),
			},
			expectedErr: false,
		},
		"day windows over 1 day": {
			start:      dayStart,
			end:        dayEnd,
			windowSize: timeutil.Day,
			expected: []Window{
				NewClosedWindow(dayStart, dayEnd),
			},
			expectedErr: false,
		},
		"day windows over 3 days": {
			start:      dayStart,
			end:        dayStart.Add(timeutil.Day * 3),
			windowSize: timeutil.Day,
			expected: []Window{
				NewClosedWindow(dayStart, dayStart.Add(timeutil.Day)),
				NewClosedWindow(dayStart.Add(timeutil.Day), dayStart.Add(timeutil.Day*2)),
				NewClosedWindow(dayStart.Add(timeutil.Day*2), dayStart.Add(timeutil.Day*3)),
			},
			expectedErr: false,
		},
		"day windows off day grid": {
			start:      dayStart.Add(time.Hour),
			end:        dayEnd.Add(time.Hour),
			windowSize: timeutil.Day,
			expected: []Window{
				NewClosedWindow(dayStart.Add(time.Hour), dayEnd.Add(time.Hour)),
			},
			expectedErr: false,
		},
		"day windows range not divisible by day": {
			start:      dayStart,
			end:        dayEnd.Add(time.Hour),
			windowSize: timeutil.Day,
			expected: []Window{
				NewClosedWindow(dayStart, dayEnd),
				NewClosedWindow(dayEnd, dayEnd.Add(time.Hour)),
			},
			expectedErr: false,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := GetWindowsForQueryWindow(tc.start, tc.end, tc.windowSize)
			if (err != nil) != tc.expectedErr {
				t.Errorf("GetWindowsForQueryWindow() error = %v, expectedErr %v", err, tc.expectedErr)
				return
			}
			if len(tc.expected) != len(actual) {
				t.Errorf("GetWindowsForQueryWindow() []window has incorrect length expected: %d, actual: %d", len(tc.expected), len(actual))
			}
			for i, actualWindow := range actual {
				expectedWindow := tc.expected[i]
				if !actualWindow.Equal(expectedWindow) {
					t.Errorf("GetWindowsForQueryWindow() window at index %d were not equal expected: %s, actual %s", i, expectedWindow.String(), actualWindow)
				}
			}
		})
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	t1 := time.Date(2023, 03, 11, 01, 29, 15, 0, time.UTC)
	t2 := t1.Add(8 * time.Minute)
	cases := []struct {
		w Window
	}{
		{
			w: NewClosedWindow(t1, t2),
		},
		{
			w: NewWindow(&t1, nil),
		},
		{
			w: NewWindow(nil, &t2),
		},
		{
			w: NewWindow(nil, nil),
		},
	}

	for _, c := range cases {
		name := c.w.String()
		t.Run(name, func(t *testing.T) {
			marshaled, err := json.Marshal(c.w)
			if err != nil {
				t.Fatalf("marshaling: %s", err)
			}

			var unmarshaledW Window
			err = json.Unmarshal(marshaled, &unmarshaledW)
			if err != nil {
				t.Fatalf("unmarshaling: %s", err)
			}

			if diff := cmp.Diff(c.w, unmarshaledW); len(diff) > 0 {
				t.Errorf(diff)
			}
		})
	}
}

func TestBoundaryErrorIs(t *testing.T) {
	baseError := &BoundaryError{Message: "cde"}
	singleWrapError := fmt.Errorf("wrap: %w", &BoundaryError{Message: "abc"})

	if errors.Is(singleWrapError, baseError) {
		t.Logf("Single wrap success")
	} else {
		t.Errorf("Single wrap failure: %s != %s", baseError, singleWrapError)
	}

	multiWrapError := fmt.Errorf("wrap: %w", &BoundaryError{Message: "abc"})
	multiWrapError = fmt.Errorf("wrap x2: %w", multiWrapError)
	multiWrapError = fmt.Errorf("wrap x3: %w", multiWrapError)

	if errors.Is(multiWrapError, baseError) {
		t.Logf("Multi wrap success")
	} else {
		t.Errorf("Multi wrap failure: %s != %s", baseError, multiWrapError)
	}
}
func TestWindowGetWeeklyWindows(t *testing.T) {
	testCases := []struct {
		name     string
		start    time.Time
		end      time.Time
		expected []Window
	}{
		{
			name:  "Single week",
			start: time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC),
			end:   time.Date(2024, 9, 8, 0, 0, 0, 0, time.UTC),
			expected: []Window{
				NewClosedWindow(
					time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 9, 8, 0, 0, 0, 0, time.UTC),
				),
			},
		},
		{
			name:  "Multiple weeks",
			start: time.Date(2024, 10, 6, 0, 0, 0, 0, time.UTC),
			end:   time.Date(2024, 10, 20, 0, 0, 0, 0, time.UTC),
			expected: []Window{
				NewClosedWindow(
					time.Date(2024, 10, 6, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 10, 13, 0, 0, 0, 0, time.UTC),
				),
				NewClosedWindow(
					time.Date(2024, 10, 13, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 10, 20, 0, 0, 0, 0, time.UTC),
				),
			},
		},
		{
			name:  "Partial starting week",
			start: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			end:   time.Date(2024, 10, 13, 0, 0, 0, 0, time.UTC),
			expected: []Window{
				NewClosedWindow(
					time.Date(2024, 9, 29, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 10, 6, 0, 0, 0, 0, time.UTC),
				),
				NewClosedWindow(
					time.Date(2024, 10, 6, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 10, 13, 0, 0, 0, 0, time.UTC),
				),
			},
		},
		{
			name:  "Partial ending week",
			start: time.Date(2024, 10, 6, 0, 0, 0, 0, time.UTC),
			end:   time.Date(2024, 10, 7, 0, 0, 0, 0, time.UTC),
			expected: []Window{
				NewClosedWindow(
					time.Date(2024, 10, 6, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 10, 13, 0, 0, 0, 0, time.UTC),
				),
			},
		},
		{
			name:  "multiple weeks, partial start and end",
			start: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			end:   time.Date(2024, 11, 21, 0, 0, 0, 0, time.UTC),
			expected: []Window{
				NewClosedWindow(
					time.Date(2024, 9, 29, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 10, 6, 0, 0, 0, 0, time.UTC),
				),
				NewClosedWindow(
					time.Date(2024, 10, 6, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 10, 13, 0, 0, 0, 0, time.UTC),
				),
				NewClosedWindow(
					time.Date(2024, 10, 13, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 10, 20, 0, 0, 0, 0, time.UTC),
				),
				NewClosedWindow(
					time.Date(2024, 10, 20, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 10, 27, 0, 0, 0, 0, time.UTC),
				),
				NewClosedWindow(
					time.Date(2024, 10, 27, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 11, 3, 0, 0, 0, 0, time.UTC),
				),
				NewClosedWindow(
					time.Date(2024, 11, 3, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 11, 10, 0, 0, 0, 0, time.UTC),
				),
				NewClosedWindow(
					time.Date(2024, 11, 10, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 11, 17, 0, 0, 0, 0, time.UTC),
				),
				NewClosedWindow(
					time.Date(2024, 11, 17, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 11, 24, 0, 0, 0, 0, time.UTC),
				),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w := NewClosedWindow(tc.start, tc.end)
			actual := w.getWeeklyWindows()
			if len(actual) != len(tc.expected) {
				t.Fatalf("expected %d windows, got %d", len(tc.expected), len(actual))
			}
			for i, expectedWindow := range tc.expected {
				if !actual[i].Equal(expectedWindow) {
					t.Errorf("expected window %s, got %s", expectedWindow, actual[i])
				}
			}
		})
	}
}
