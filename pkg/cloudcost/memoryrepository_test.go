package cloudcost

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

func TestMemoryRepository_Get(t *testing.T) {
	defaultStart := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	defaultEnd := defaultStart.Add(timeutil.Day)
	defaultData := map[string]map[time.Time]*opencost.CloudCostSet{
		"key-1": {
			defaultStart: DefaultMockCloudCostSet(defaultStart, defaultEnd, "aws", "key-1"),
		},
	}
	tests := map[string]struct {
		data      map[string]map[time.Time]*opencost.CloudCostSet
		startTime time.Time
		key       string
		want      *opencost.CloudCostSet
		wantErr   bool
	}{
		"No Data": {
			data:      map[string]map[time.Time]*opencost.CloudCostSet{},
			startTime: defaultStart,
			key:       "key-1",
			want:      nil,
			wantErr:   false,
		},
		"has data": {
			data:      defaultData,
			startTime: defaultStart,
			key:       "key-1",
			want:      DefaultMockCloudCostSet(defaultStart, defaultEnd, "aws", "key-1"),
			wantErr:   false,
		},
		"wrong key": {
			data:      defaultData,
			startTime: defaultStart,
			key:       "key-2",
			want:      nil,
			wantErr:   false,
		},
		"wrong time": {
			data:      defaultData,
			startTime: defaultEnd,
			key:       "key-1",
			want:      nil,
			wantErr:   false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			m := &MemoryRepository{
				data: tt.data,
			}
			got, err := m.Get(tt.startTime, tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMemoryRepository_Has(t *testing.T) {
	defaultStart := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	defaultEnd := defaultStart.Add(timeutil.Day)
	defaultData := map[string]map[time.Time]*opencost.CloudCostSet{
		"key-1": {
			defaultStart: DefaultMockCloudCostSet(defaultStart, defaultEnd, "aws", "key-1"),
		},
	}
	tests := map[string]struct {
		data      map[string]map[time.Time]*opencost.CloudCostSet
		startTime time.Time
		key       string
		want      bool
		wantErr   bool
	}{
		"No Data": {
			data:      map[string]map[time.Time]*opencost.CloudCostSet{},
			startTime: defaultStart,
			key:       "key-1",
			want:      false,
			wantErr:   false,
		},
		"has data": {
			data:      defaultData,
			startTime: defaultStart,
			key:       "key-1",
			want:      true,
			wantErr:   false,
		},
		"wrong key": {
			data:      defaultData,
			startTime: defaultStart,
			key:       "key-2",
			want:      false,
			wantErr:   false,
		},
		"wrong time": {
			data:      defaultData,
			startTime: defaultEnd,
			key:       "key-1",
			want:      false,
			wantErr:   false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			m := &MemoryRepository{
				data: tt.data,
			}
			got, err := m.Has(tt.startTime, tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Has() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Has() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMemoryRepository_Keys(t *testing.T) {

	tests := map[string]struct {
		data    map[string]map[time.Time]*opencost.CloudCostSet
		want    []string
		wantErr bool
	}{
		"empty": {
			data:    map[string]map[time.Time]*opencost.CloudCostSet{},
			want:    []string{},
			wantErr: false,
		},
		"one-key": {
			data: map[string]map[time.Time]*opencost.CloudCostSet{
				"key-1": nil,
			},
			want:    []string{"key-1"},
			wantErr: false,
		},
		"two-key": {
			data: map[string]map[time.Time]*opencost.CloudCostSet{
				"key-1": nil,
				"key-2": {
					time.Now():        nil,
					time.Now().Add(1): nil,
				},
			},
			want:    []string{"key-1", "key-2"},
			wantErr: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			m := &MemoryRepository{
				data: tt.data,
			}
			got, err := m.Keys()
			if (err != nil) != tt.wantErr {
				t.Errorf("Keys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			sort.Strings(got)
			sort.Strings(tt.want)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Keys() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMemoryRepository_Put(t *testing.T) {
	defaultStart := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	defaultEnd := defaultStart.Add(timeutil.Day)

	tests := map[string]struct {
		data    map[string]map[time.Time]*opencost.CloudCostSet
		input   *opencost.CloudCostSet
		want    map[string]map[time.Time]*opencost.CloudCostSet
		wantErr bool
	}{

		"nil set": {
			data:    map[string]map[time.Time]*opencost.CloudCostSet{},
			input:   nil,
			want:    map[string]map[time.Time]*opencost.CloudCostSet{},
			wantErr: true,
		},
		"invalid window": {
			data: map[string]map[time.Time]*opencost.CloudCostSet{},
			input: &opencost.CloudCostSet{
				CloudCosts:  nil,
				Window:      opencost.Window{},
				Integration: "key-1",
			},
			want:    map[string]map[time.Time]*opencost.CloudCostSet{},
			wantErr: true,
		},
		"invalid key": {
			data: map[string]map[time.Time]*opencost.CloudCostSet{},
			input: &opencost.CloudCostSet{
				CloudCosts:  nil,
				Window:      opencost.NewClosedWindow(defaultStart, defaultEnd),
				Integration: "",
			},
			want:    map[string]map[time.Time]*opencost.CloudCostSet{},
			wantErr: true,
		},
		"valid input": {
			data:  map[string]map[time.Time]*opencost.CloudCostSet{},
			input: DefaultMockCloudCostSet(defaultStart, defaultEnd, "aws", "key-1"),
			want: map[string]map[time.Time]*opencost.CloudCostSet{
				"key-1": {
					defaultStart: DefaultMockCloudCostSet(defaultStart, defaultEnd, "aws", "key-1"),
				},
			},
			wantErr: false,
		},
		"overwrite": {
			data: map[string]map[time.Time]*opencost.CloudCostSet{
				"key-1": {
					defaultStart: DefaultMockCloudCostSet(defaultStart, defaultEnd, "gcp", "key-1"),
				},
			},
			input: DefaultMockCloudCostSet(defaultStart, defaultEnd, "aws", "key-1"),
			want: map[string]map[time.Time]*opencost.CloudCostSet{
				"key-1": {
					defaultStart: DefaultMockCloudCostSet(defaultStart, defaultEnd, "aws", "key-1"),
				},
			},
			wantErr: false,
		},
		"invalid overwrite": {
			data: map[string]map[time.Time]*opencost.CloudCostSet{
				"key-1": {
					defaultStart: DefaultMockCloudCostSet(defaultStart, defaultEnd, "gcp", "key-1"),
				},
			},
			input: &opencost.CloudCostSet{
				Window:      opencost.NewWindow(&defaultStart, nil),
				Integration: "key-1",
			},
			want: map[string]map[time.Time]*opencost.CloudCostSet{
				"key-1": {
					defaultStart: DefaultMockCloudCostSet(defaultStart, defaultEnd, "gcp", "key-1"),
				},
			},
			wantErr: true,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			m := &MemoryRepository{data: tt.data}

			if err := m.Put(tt.input); (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !reflect.DeepEqual(m.data, tt.want) {
				t.Errorf("Put() got = %v, want %v", m.data, tt.want)
			}
		})
	}
}

func TestMemoryRepository_Expire(t *testing.T) {
	dayOne := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	dayTwo := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	dayThree := time.Date(2023, 1, 3, 0, 0, 0, 0, time.UTC)
	tests := map[string]struct {
		data    map[string]map[time.Time]*opencost.CloudCostSet
		limit   time.Time
		want    map[string]map[time.Time]*opencost.CloudCostSet
		wantErr bool
	}{
		"no expire": {
			data: map[string]map[time.Time]*opencost.CloudCostSet{
				"key-1": {
					dayTwo: nil,
				},
			},
			limit: dayOne,
			want: map[string]map[time.Time]*opencost.CloudCostSet{
				"key-1": {
					dayTwo: nil,
				},
			},
			wantErr: false,
		},
		"limit match": {
			data: map[string]map[time.Time]*opencost.CloudCostSet{
				"key-1": {
					dayTwo: nil,
				},
			},
			limit: dayTwo,
			want: map[string]map[time.Time]*opencost.CloudCostSet{
				"key-1": {
					dayTwo: nil,
				},
			},
			wantErr: false,
		},
		"single expire": {
			data: map[string]map[time.Time]*opencost.CloudCostSet{
				"key-1": {
					dayTwo: nil,
				},
			},
			limit:   dayThree,
			want:    map[string]map[time.Time]*opencost.CloudCostSet{},
			wantErr: false,
		},
		"one key expire": {
			data: map[string]map[time.Time]*opencost.CloudCostSet{
				"key-1": {
					dayOne: nil,
					dayTwo: nil,
				},
				"key-2": {
					dayOne: nil,
				},
			},
			limit: dayTwo,
			want: map[string]map[time.Time]*opencost.CloudCostSet{
				"key-1": {
					dayTwo: nil,
				},
			},
			wantErr: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			m := &MemoryRepository{
				data: tt.data,
			}
			if err := m.Expire(tt.limit); (err != nil) != tt.wantErr {
				t.Errorf("Expire() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !reflect.DeepEqual(m.data, tt.want) {
				t.Errorf("Expire() got = %v, want %v", m.data, tt.want)
			}

		})
	}
}
