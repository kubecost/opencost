package stringutil

import (
	"fmt"
	"math/rand"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
)

var oldBank sync.Map

// This is the old implementation of the string bank to use for comparison benchmarks
func BankLegacy(s string) string {
	ss, _ := oldBank.LoadOrStore(s, s)
	return ss.(string)
}

func ClearBankLegacy() {
	oldBank = sync.Map{}
}

func copyString(s string) string {
	return string([]byte(s))
}

func generateBenchData(totalStrings, totalUnique int) []string {
	randStrings := make([]string, 0, totalStrings)

	// create totalUnique unique strings
	for i := 0; i < totalUnique; i++ {
		randStrings = append(randStrings, fmt.Sprintf("%s/%s/%s", RandSeq(10), RandSeq(10), RandSeq(10)))
	}

	// set the seed such that the resulting "remainder" strings are deterministic for each bench
	rand.Seed(1523942)

	// append a random selection from 0-totalUnique to the list.
	for i := 0; i < totalStrings-totalUnique; i++ {
		randStrings = append(randStrings, copyString(randStrings[rand.Intn(totalUnique)]))
	}

	// shuffle the list of strings
	rand.Shuffle(totalStrings, func(i, j int) { randStrings[i], randStrings[j] = randStrings[j], randStrings[i] })

	return randStrings
}

func benchmarkLegacyStringBank(b *testing.B, totalStrings, totalUnique int) {
	b.StopTimer()
	randStrings := generateBenchData(totalStrings, totalUnique)

	for i := 0; i < b.N; i++ {
		b.StartTimer()
		for b := 0; b < totalStrings; b++ {
			BankLegacy(randStrings[b])
		}
		b.StopTimer()
		ClearBankLegacy()
		runtime.GC()
		debug.FreeOSMemory()
	}
}

func benchmarkStringBank(b *testing.B, totalStrings, totalUnique int, useBankFunc bool) {
	b.StopTimer()
	randStrings := generateBenchData(totalStrings, totalUnique)

	for i := 0; i < b.N; i++ {
		b.StartTimer()
		for b := 0; b < totalStrings; b++ {
			if useBankFunc {
				BankFunc(randStrings[b], func() string { return randStrings[b] })
			} else {
				Bank(randStrings[b])
			}
		}
		b.StopTimer()
		ClearBank()
		runtime.GC()
		debug.FreeOSMemory()
	}
}

func BenchmarkLegacyStringBank90PercentDuplicate(b *testing.B) {
	benchmarkLegacyStringBank(b, 1_000_000, 100_000)
}

func BenchmarkLegacyStringBank75PercentDuplicate(b *testing.B) {
	benchmarkLegacyStringBank(b, 1_000_000, 250_000)
}

func BenchmarkLegacyStringBank50PercentDuplicate(b *testing.B) {
	benchmarkLegacyStringBank(b, 1_000_000, 100_000)
}

func BenchmarkLegacyStringBank25PercentDuplicate(b *testing.B) {
	benchmarkLegacyStringBank(b, 1_000_000, 750_000)
}

func BenchmarkLegacyStringBankNoDuplicate(b *testing.B) {
	benchmarkLegacyStringBank(b, 1_000_000, 1_000_000)
}

func BenchmarkStringBank90PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, 1_000_000, 100_000, false)
}

func BenchmarkStringBank75PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, 1_000_000, 250_000, false)
}

func BenchmarkStringBank50PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, 1_000_000, 100_000, false)
}

func BenchmarkStringBank25PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, 1_000_000, 750_000, false)
}

func BenchmarkStringBankNoDuplicate(b *testing.B) {
	benchmarkStringBank(b, 1_000_000, 1_000_000, false)
}

func BenchmarkStringBankFunc90PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, 1_000_000, 100_000, false)
}

func BenchmarkStringBankFunc75PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, 1_000_000, 250_000, false)
}

func BenchmarkStringBankFunc50PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, 1_000_000, 100_000, false)
}

func BenchmarkStringBankFunc25PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, 1_000_000, 750_000, false)
}

func BenchmarkStringBankFuncNoDuplicate(b *testing.B) {
	benchmarkStringBank(b, 1_000_000, 1_000_000, false)
}
