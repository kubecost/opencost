package legacy_test

// import (
// 	"github.com/opencost/opencost/pkg/kubecost"
// 	"testing"
// 	"time"
// )

// type windowedImpl struct {
// 	opencost.Window
// }

// func (w *windowedImpl) GetWindow() opencost.Window {
// 	return w.Window
// }

// func newWindowedImpl(start, end *time.Time) *windowedImpl {
// 	return &windowedImpl{opencost.NewWindow(start, end)}
// }

// func Test_WindowContains_Matches(t *testing.T) {
// 	noon := time.Date(2022, 9, 29, 12, 0, 0, 0, time.UTC)
// 	one := noon.Add(time.Hour)
// 	two := one.Add(time.Hour)
// 	three := two.Add(time.Hour)
// 	cases := map[string]struct {
// 		windowed *windowedImpl
// 		filter   Filter[*windowedImpl]
// 		expected bool
// 	}{
// 		"fully contains": {
// 			windowed: newWindowedImpl(&one, &two),
// 			filter: WindowCondition[*windowedImpl]{
// 				Window: opencost.NewWindow(&noon, &three),
// 				Op:     WindowContains,
// 			},

// 			expected: true,
// 		},
// 		"window matches": {
// 			windowed: newWindowedImpl(&one, &two),
// 			filter: WindowCondition[*windowedImpl]{
// 				Window: opencost.NewWindow(&one, &two),
// 				Op:     WindowContains,
// 			},

// 			expected: true,
// 		},
// 		"contains start": {
// 			windowed: newWindowedImpl(&one, &three),
// 			filter: WindowCondition[*windowedImpl]{
// 				Window: opencost.NewWindow(&noon, &two),
// 				Op:     WindowContains,
// 			},

// 			expected: false,
// 		},
// 		"contains end": {
// 			windowed: newWindowedImpl(&noon, &two),
// 			filter: WindowCondition[*windowedImpl]{
// 				Window: opencost.NewWindow(&one, &three),
// 				Op:     WindowContains,
// 			},

// 			expected: false,
// 		},
// 		"window start = filter end": {
// 			windowed: newWindowedImpl(&one, &two),
// 			filter: WindowCondition[*windowedImpl]{
// 				Window: opencost.NewWindow(&noon, &one),
// 				Op:     WindowContains,
// 			},

// 			expected: false,
// 		},
// 		"window end = filter start": {
// 			windowed: newWindowedImpl(&noon, &one),
// 			filter: WindowCondition[*windowedImpl]{
// 				Window: opencost.NewWindow(&one, &two),
// 				Op:     WindowContains,
// 			},

// 			expected: false,
// 		},
// 		"window before": {
// 			windowed: newWindowedImpl(&noon, &one),
// 			filter: WindowCondition[*windowedImpl]{
// 				Window: opencost.NewWindow(&two, &three),
// 				Op:     WindowContains,
// 			},

// 			expected: false,
// 		},
// 		"window after": {
// 			windowed: newWindowedImpl(&two, &three),
// 			filter: WindowCondition[*windowedImpl]{
// 				Window: opencost.NewWindow(&noon, &one),
// 				Op:     WindowContains,
// 			},

// 			expected: false,
// 		},
// 	}

// 	for name, c := range cases {
// 		result := c.filter.Matches(c.windowed)

// 		if result != c.expected {
// 			t.Errorf("%s: expected %t, got %t", name, c.expected, result)
// 		}
// 	}
// }
