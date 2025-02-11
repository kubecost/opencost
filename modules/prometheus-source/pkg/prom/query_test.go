package prom

import (
	"reflect"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/prometheus/client_golang/api"
)

func TestWarningsFrom(t *testing.T) {
	var results any = map[string]interface{}{
		"status": "success",
		"warnings": []string{
			"Warning #1",
			"Warning #2",
		},
	}

	warnings := warningsFrom(results)
	if len(warnings) != 2 {
		t.Errorf("Unexpected warnings length: %d, Expected 2.", len(warnings))
	}

	if warnings[0] != "Warning #1" {
		t.Errorf("Unexpected first warning: %s", warnings[0])
	}
	if warnings[1] != "Warning #2" {
		t.Errorf("Unexpected second warning: %s", warnings[1])
	}
}

func TestContext_isRequestStepAligned(t *testing.T) {
	type fields struct {
		Client         api.Client
		name           string
		errorCollector *source.QueryErrorCollector
	}
	type args struct {
		start time.Time
		end   time.Time
		step  time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name:   "Test with times that are not step aligned to the hour",
			fields: fields{},
			args: args{
				start: time.Date(2022, 11, 7, 4, 59, 30, 0, time.UTC),
				end:   time.Date(2022, 11, 8, 4, 59, 30, 0, time.UTC),
				step:  time.Hour,
			},
			want: false,
		},
		{
			name:   "Test with times that are step aligned to the hour",
			fields: fields{},
			args: args{
				start: time.Date(2022, 11, 7, 4, 0, 0, 0, time.UTC),
				end:   time.Date(2022, 11, 8, 4, 0, 0, 0, time.UTC),
				step:  time.Hour,
			},
			want: true,
		},
		{
			name:   "Test with times where start is aligned to the hour but end is not",
			fields: fields{},
			args: args{
				start: time.Date(2022, 11, 7, 4, 0, 0, 0, time.UTC),
				end:   time.Date(2022, 11, 8, 4, 59, 0, 0, time.UTC),
				step:  time.Hour,
			},
			want: false,
		},
		{
			name:   "Test with times where end is aligned to the hour but start is not",
			fields: fields{},
			args: args{
				start: time.Date(2022, 11, 7, 4, 59, 0, 0, time.UTC),
				end:   time.Date(2022, 11, 8, 4, 0, 0, 0, time.UTC),
				step:  time.Hour,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &Context{
				Client:         tt.fields.Client,
				name:           tt.fields.name,
				errorCollector: tt.fields.errorCollector,
			}
			if got := ctx.isRequestStepAligned(tt.args.start, tt.args.end, tt.args.step); got != tt.want {
				t.Errorf("isRequestStepAligned() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestContext_alignWindow(t *testing.T) {
	type fields struct {
		Client         api.Client
		name           string
		errorCollector *source.QueryErrorCollector
	}
	type args struct {
		start time.Time
		end   time.Time
		step  time.Duration
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantStart time.Time
		wantEnd   time.Time
	}{
		{
			name:   "Do not update the start and end when step-aligned",
			fields: fields{},
			args: args{
				start: time.Date(2022, 11, 7, 4, 0, 0, 0, time.UTC),
				end:   time.Date(2022, 11, 8, 4, 0, 0, 0, time.UTC),
				step:  time.Hour,
			},
			wantStart: time.Date(2022, 11, 7, 4, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2022, 11, 8, 4, 0, 0, 0, time.UTC),
		},
		{
			name:   "Update start to be step-aligned and leave end the same",
			fields: fields{},
			args: args{
				start: time.Date(2022, 11, 7, 4, 59, 0, 0, time.UTC),
				end:   time.Date(2022, 11, 8, 4, 0, 0, 0, time.UTC),
				step:  time.Hour,
			},
			wantStart: time.Date(2022, 11, 7, 4, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2022, 11, 8, 4, 0, 0, 0, time.UTC),
		},
		{
			name:   "Update end to be step-aligned and leave start the same",
			fields: fields{},
			args: args{
				start: time.Date(2022, 11, 7, 4, 0, 0, 0, time.UTC),
				end:   time.Date(2022, 11, 8, 4, 59, 0, 0, time.UTC),
				step:  time.Hour,
			},
			wantStart: time.Date(2022, 11, 7, 4, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2022, 11, 8, 4, 0, 0, 0, time.UTC),
		},
		{
			name:   "Update start and end to be step-aligned",
			fields: fields{},
			args: args{
				start: time.Date(2022, 11, 7, 4, 59, 0, 0, time.UTC),
				end:   time.Date(2022, 11, 8, 4, 59, 0, 0, time.UTC),
				step:  time.Hour,
			},
			wantStart: time.Date(2022, 11, 7, 4, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2022, 11, 8, 4, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &Context{
				Client:         tt.fields.Client,
				name:           tt.fields.name,
				errorCollector: tt.fields.errorCollector,
			}
			got, got1 := ctx.alignWindow(tt.args.start, tt.args.end, tt.args.step)
			if !reflect.DeepEqual(got, tt.wantStart) {
				t.Errorf("alignWindow() got = %v, want %v", got, tt.wantStart)
			}
			if !reflect.DeepEqual(got1, tt.wantEnd) {
				t.Errorf("alignWindow() got1 = %v, want %v", got1, tt.wantEnd)
			}
		})
	}
}
