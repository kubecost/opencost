package timeutil

import (
	"fmt"
	"testing"
	"time"
)

func TestProfileDataSeries_NilOrZero(t *testing.T) {
	pds1 := NewProfileDataSeries("test1", 0)
	fmt.Println(pds1.String())

	pds2 := NewProfileDataSeries("test2", 0)
	pds2.Start()
	fmt.Println(pds2.String())

	pds2.Stop()
	fmt.Println(pds2.String())
}

func TestProfileDataSeries_Series(t *testing.T) {
	pds := NewProfileDataSeries("test", 3)

	pds.Start()

	time.Sleep(10 * time.Millisecond)

	pds.Step("step1")

	time.Sleep(100 * time.Millisecond)

	pds.Step("step2")

	time.Sleep(5 * time.Millisecond)

	pds.Step("step3")

	time.Sleep(1 * time.Millisecond)

	pds.Stop()

	fmt.Println(len(pds.Series))
	for i, p := range pds.Series {
		if p == nil {
			fmt.Printf("%d nil\n", i)
		} else {
			fmt.Printf("%d %s %v\n", i, p.Name, p.Time)
		}
	}

	fmt.Println(pds.String())
}
