package stringutil

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

const (
	_ = 1 << (10 * iota)
	// KiB is bytes per Kibibyte
	KiB
	// MiB is bytes per Mebibyte
	MiB
	// GiB is bytes per Gibibyte
	GiB
	// TiB is bytes per Tebibyte
	TiB
)

var alpha = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var alphanumeric = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

type stringBank struct {
	lock sync.Mutex
	m    map[string]string
}

func newStringBank() *stringBank {
	return &stringBank{
		m: make(map[string]string),
	}
}

func (sb *stringBank) LoadOrStore(key, value string) (string, bool) {
	sb.lock.Lock()

	if v, ok := sb.m[key]; ok {
		sb.lock.Unlock()
		return v, ok
	}

	sb.m[key] = value
	sb.lock.Unlock()
	return value, false
}

func (sb *stringBank) LoadOrStoreFunc(key string, f func() string) (string, bool) {
	sb.lock.Lock()

	if v, ok := sb.m[key]; ok {
		sb.lock.Unlock()
		return v, ok
	}

	// create the key and value using the func (the key could be deallocated later)
	value := f()
	sb.m[value] = value
	sb.lock.Unlock()
	return value, false
}

func (sb *stringBank) Clear() {
	sb.lock.Lock()
	sb.m = make(map[string]string)
	sb.lock.Unlock()
}

// stringBank is an unbounded string cache that is thread-safe. It is especially useful if
// storing a large frequency of dynamically allocated duplicate strings.
var strings = newStringBank() // sync.Map

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Bank will return a non-copy of a string if it has been used before. Otherwise, it will store
// the string as the unique instance.
func Bank(s string) string {
	ss, _ := strings.LoadOrStore(s, s)
	return ss
}

// BankFunc will use the provided s string to check for an existing allocation of the string. However,
// if no allocation exists, the f parameter will be used to create the string and store in the bank.
func BankFunc(s string, f func() string) string {
	ss, _ := strings.LoadOrStoreFunc(s, f)
	return ss
}

func ClearBank() {
	strings.Clear()
}

// RandSeq generates a pseudo-random alphabetic string of the given length
func RandSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = alpha[rand.Intn(len(alpha))] // #nosec No need for a cryptographic strength random here
	}
	return string(b)
}

// FormatBytes takes a number of bytes and formats it as a string
func FormatBytes(numBytes int64) string {
	if numBytes > TiB {
		return fmt.Sprintf("%.2fTiB", float64(numBytes)/TiB)
	}
	if numBytes > GiB {
		return fmt.Sprintf("%.2fGiB", float64(numBytes)/GiB)
	}
	if numBytes > MiB {
		return fmt.Sprintf("%.2fMiB", float64(numBytes)/MiB)
	}
	if numBytes > KiB {
		return fmt.Sprintf("%.2fKiB", float64(numBytes)/KiB)
	}
	return fmt.Sprintf("%dB", numBytes)
}

// FormatUTCOffset converts a duration to a string of format "-07:00"
func FormatUTCOffset(dur time.Duration) string {
	utcOffSig := "+"
	if dur.Hours() < 0 {
		utcOffSig = "-"
	}
	utcOffHrs := int(math.Trunc(math.Abs(dur.Hours())))
	utcOffMin := int(math.Abs(dur.Minutes())) - (utcOffHrs * 60)

	return fmt.Sprintf("%s%02d:%02d", utcOffSig, utcOffHrs, utcOffMin)
}

// StringSlicesEqual checks if two string slices with arbitrary order have the same elements
func StringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	// Build maps for each slice that counts each unique instance
	leftMap := make(map[string]int, len(left))
	for _, str := range left {
		count, ok := leftMap[str]
		if ok {
			leftMap[str] = count + 1
		} else {
			leftMap[str] = 1
		}
	}
	rightMap := make(map[string]int, len(right))
	for _, str := range right {
		count, ok := rightMap[str]
		if ok {
			rightMap[str] = count + 1
		} else {
			rightMap[str] = 1
		}
	}
	// check that each unique key has the same count in each slice
	for key, count := range leftMap {
		if rightMap[key] != count {
			return false
		}
	}
	return true
}

// DeleteEmptyStringsFromArray removes the empty strings from an array.
func DeleteEmptyStringsFromArray(input []string) (output []string) {
	for _, str := range input {
		if str != "" {
			output = append(output, str)
		}
	}
	return
}
