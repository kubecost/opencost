package kubecost

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kubecost/cost-model/pkg/env"
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
}

func TestParseWindowUTC_ISO8601(t *testing.T) {
	ago30s := time.Now().UTC().Add(-30 * time.Second)
	ago12h := time.Now().UTC().Add(-12 * time.Hour)
	ago30h := time.Now().UTC().Add(-30 * time.Hour)

	pt24h, err := ParseWindowUTC("PT24H")
	if err != nil {
		t.Fatalf(`unexpected error parsing "PT1H": %s`, err)
	}
	if pt24h.Duration().Hours() != 24 {
		t.Fatalf(`expect: window "PT24H" to have duration 24 hour; actual: %f hours`, pt24h.Duration().Hours())
	}
	if !pt24h.Contains(ago12h) {
		t.Fatalf(`expect: window "PT24H" to contain ago12h; actual: %s -- %s`, pt24h, ago12h)
	}
	if pt24h.Contains(ago30h) {
		t.Fatalf(`expect: window "PT24H" to NOT contain ago30h; actual: %s`, pt24h)
	}

	pt1m, err := ParseWindowUTC("PT1M")
	if err != nil {
		t.Fatalf(`unexpected error parsing "PT1M": %s`, err)
	}
	if pt1m.Duration().Minutes() != 1 {
		t.Fatalf(`expect: window "PT1M" to have duration 1 hour; actual: %f hours`, pt1m.Duration().Hours())
	}
	if !pt1m.Contains(ago30s) {
		t.Fatalf(`expect: window "PT1M" to contain ago30m; actual: %s`, pt1m)
	}
	if pt1m.Contains(ago12h) {
		t.Fatalf(`expect: window "PT1M" to NOT contain ago12h; actual: %s`, pt1m)
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
	if !month.End().Before(time.Now().UTC()) {
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
	if !dur2d.Contains(ago36h) {
		t.Fatalf(`expect: window "2d" to contain 36 hours ago; actual: %s doesn't contain %s`, dur2d, ago36h)
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
		t.Fatalf(`unexpected error parsing "3h": %s`, err)
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
		t.Fatalf(`unexpected error parsing "10m": %s`, err)
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

func TestWindow_DurationOffsetForPrometheus(t *testing.T) {
	// Set-up and tear-down
	thanosEnabled := env.GetBool(env.ThanosEnabledEnvVar, false)
	defer env.SetBool(env.ThanosEnabledEnvVar, thanosEnabled)

	// Test for Prometheus (env.IsThanosEnabled() == false)
	env.SetBool(env.ThanosEnabledEnvVar, false)
	if env.IsThanosEnabled() {
		t.Fatalf("expected env.IsThanosEnabled() == false")
	}

	w, err := ParseWindowUTC("1d")
	if err != nil {
		t.Fatalf(`unexpected error parsing "1d": %s`, err)
	}
	dur, off, err := w.DurationOffsetForPrometheus()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if dur != "1d" {
		t.Fatalf(`expect: window to be "1d"; actual: "%s"`, dur)
	}
	if off != "" {
		t.Fatalf(`expect: offset to be ""; actual: "%s"`, off)
	}

	w, err = ParseWindowUTC("2h")
	if err != nil {
		t.Fatalf(`unexpected error parsing "2h": %s`, err)
	}
	dur, off, err = w.DurationOffsetForPrometheus()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if dur != "2h" {
		t.Fatalf(`expect: window to be "2h"; actual: "%s"`, dur)
	}
	if off != "" {
		t.Fatalf(`expect: offset to be ""; actual: "%s"`, off)
	}

	w, err = ParseWindowUTC("10m")
	if err != nil {
		t.Fatalf(`unexpected error parsing "10m": %s`, err)
	}
	dur, off, err = w.DurationOffsetForPrometheus()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
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
	dur, off, err = w.DurationOffsetForPrometheus()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if dur != "1441m" {
		t.Fatalf(`expect: window to be "1441m"; actual: "%s"`, dur)
	}
	if !strings.HasPrefix(off, " offset ") {
		t.Fatalf(`expect: offset to start with " offset "; actual: "%s"`, off)
	}

	w, err = ParseWindowUTC("yesterday")
	if err != nil {
		t.Fatalf(`unexpected error parsing "yesterday": %s`, err)
	}
	dur, off, err = w.DurationOffsetForPrometheus()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if dur != "1d" {
		t.Fatalf(`expect: window to be "1d"; actual: "%s"`, dur)
	}
	if !strings.HasPrefix(off, " offset ") {
		t.Fatalf(`expect: offset to start with " offset "; actual: "%s"`, off)
	}

	// Test for Thanos (env.IsThanosEnabled() == true)
	env.SetBool(env.ThanosEnabledEnvVar, true)
	if !env.IsThanosEnabled() {
		t.Fatalf("expected env.IsThanosEnabled() == true")
	}

	w, err = ParseWindowUTC("1d")
	if err != nil {
		t.Fatalf(`unexpected error parsing "1d": %s`, err)
	}
	dur, off, err = w.DurationOffsetForPrometheus()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if dur != "21h" {
		t.Fatalf(`expect: window to be "21d"; actual: "%s"`, dur)
	}
	if off != " offset 3h" {
		t.Fatalf(`expect: offset to be " offset 3h"; actual: "%s"`, off)
	}

	w, err = ParseWindowUTC("2h")
	if err != nil {
		t.Fatalf(`unexpected error parsing "2h": %s`, err)
	}
	dur, off, err = w.DurationOffsetForPrometheus()
	if err == nil {
		t.Fatalf(`expected error (negative duration); got ("%s", "%s")`, dur, off)
	}

	w, err = ParseWindowUTC("10m")
	if err != nil {
		t.Fatalf(`unexpected error parsing "1d": %s`, err)
	}
	dur, off, err = w.DurationOffsetForPrometheus()
	if err == nil {
		t.Fatalf(`expected error (negative duration); got ("%s", "%s")`, dur, off)
	}

	w, err = ParseWindowUTC("1589448338,1589534798")
	if err != nil {
		t.Fatalf(`unexpected error parsing "1589448338,1589534798": %s`, err)
	}
	dur, off, err = w.DurationOffsetForPrometheus()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if dur != "1441m" {
		t.Fatalf(`expect: window to be "1441m"; actual: "%s"`, dur)
	}
	if !strings.HasPrefix(off, " offset ") {
		t.Fatalf(`expect: offset to start with " offset "; actual: "%s"`, off)
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
