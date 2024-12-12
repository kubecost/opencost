package timeutil

import (
	"fmt"
	"strings"
	"time"
)

type ProfileDataSeries struct {
	Name   string
	Series []*ProfileDatum
}

func NewProfileDataSeries(name string, steps int) *ProfileDataSeries {
	return &ProfileDataSeries{
		Name:   name,
		Series: make([]*ProfileDatum, 0, steps+2),
	}
}

func (pds *ProfileDataSeries) Start() {
	pds.Series = append(pds.Series, &ProfileDatum{
		Name: "start",
		Time: time.Now().UTC(),
	})
}

func (pds *ProfileDataSeries) Step(name string) {
	pds.Series = append(pds.Series, &ProfileDatum{
		Name: name,
		Time: time.Now().UTC(),
	})
}

func (pds *ProfileDataSeries) Stop() {
	pds.Series = append(pds.Series, &ProfileDatum{
		Name: "stop",
		Time: time.Now().UTC(),
	})
}

func (pds *ProfileDataSeries) String() string {
	if pds == nil || len(pds.Series) < 2 {
		return "--"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%s %v", pds.Name, pds.Series[len(pds.Series)-1].Time.Sub(pds.Series[0].Time)))
	for i := 1; i < len(pds.Series); i++ {
		pd := pds.Series[i]
		sb.WriteString(fmt.Sprintf(" [%s %v]", pd.Name, pds.Series[i].Time.Sub(pds.Series[i-1].Time)))
	}
	return sb.String()
}

type ProfileDatum struct {
	Name string
	Time time.Time
}
