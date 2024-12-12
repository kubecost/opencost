package util

import (
	"testing"
)

func TestGetArchType(t *testing.T) {
	type args struct {
		labels map[string]string
	}
	tests := map[string]struct {
		args  args
		want  string
		found bool
	}{
		"amd64 beta": {
			args: args{
				labels: map[string]string{
					"beta.kubernetes.io/arch": "amd64",
				},
			},
			want:  "amd64",
			found: true,
		},
		"arm64 beta": {
			args: args{
				labels: map[string]string{
					"beta.kubernetes.io/arch": "arm64",
				},
			},
			want:  "arm64",
			found: true,
		},
		"amd64": {
			args: args{
				labels: map[string]string{
					"kubernetes.io/arch": "amd64",
				},
			},
			want:  "amd64",
			found: true,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, found := GetArchType(tt.args.labels)
			if found != tt.found {
				t.Errorf("GetArchType() error = %v, wantErr %v", found, tt.found)
				return
			}
			if got != tt.want {
				t.Errorf("GetArchType() got = %v, want %v", got, tt.want)
			}
		})
	}
}
