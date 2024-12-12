package costmodel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/filemanager"
)

//go:generate moq -out moq_cloud_storage_test.go . CloudStorage:CloudStorageMock
//go:generate moq -out moq_allocation_model_test.go . AllocationModel:AllocationModelMock

func Test_UpdateCSV(t *testing.T) {
	t.Run("previous data doesn't exist, upload new data", func(t *testing.T) {
		storage := &filemanager.InMemoryFile{}
		model := &AllocationModelMock{
			DateRangeFunc: func() (time.Time, time.Time, error) {
				return time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC), nil
			},
			ComputeAllocationFunc: func(start time.Time, end time.Time, resolution time.Duration) (*opencost.AllocationSet, error) {
				return &opencost.AllocationSet{
					Allocations: map[string]*opencost.Allocation{
						"test": {
							Start:                  time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), // required for GPU metrics
							End:                    time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
							CPUCoreUsageAverage:    0.1,
							CPUCoreRequestAverage:  0.2,
							CPUCost:                0.3,
							RAMBytesUsageAverage:   0.4,
							RAMBytesRequestAverage: 0.5,
							RAMCost:                0.6,
							GPUHours:               48,
							GPUCost:                0.8,
							NetworkCost:            0.9,
							NetworkTransferBytes:   10,
							NetworkReceiveBytes:    11,
							PVs: map[opencost.PVKey]*opencost.PVAllocation{
								{
									Cluster: "test-cluster",
									Name:    "test-pv",
								}: {
									ByteHours: 48,
									Cost:      2.0,
								},
							}, // 2 PVBytes, 2 PVCost
							Properties: &opencost.AllocationProperties{
								Namespace:      "test-namespace",
								Controller:     "test-controller-name",
								ControllerKind: "test-controller-kind",
								Pod:            "test-pod",
								Container:      "test-container",
							},
						},
					},
				}, nil
			},
		}
		err := UpdateCSV(context.TODO(), storage, model, false, nil)
		require.NoError(t, err)
		// uploaded a single file with the data
		assert.Len(t, model.ComputeAllocationCalls(), 1)
		assert.Equal(t, time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), model.ComputeAllocationCalls()[0].Start)
		assert.Equal(t, time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC), model.ComputeAllocationCalls()[0].End)
		assert.Equal(t, `Date,Namespace,ControllerKind,ControllerName,Pod,Container,CPUCoreUsageAverage,CPUCoreRequestAverage,RAMBytesUsageAverage,RAMBytesRequestAverage,NetworkReceiveBytes,NetworkTransferBytes,GPUs,PVBytes,CPUCost,RAMCost,NetworkCost,PVCost,GPUCost,TotalCost
2021-01-01,test-namespace,test-controller-kind,test-controller-name,test-pod,test-container,0.1,0.2,0.4,0.5,11,10,2,2,0.3,0.6,0.9,2,0.8,4.6000000000000005
`, string(storage.Data))
	})

	t.Run("export labels", func(t *testing.T) {
		storage := &filemanager.InMemoryFile{}
		model := &AllocationModelMock{
			DateRangeFunc: func() (time.Time, time.Time, error) {
				return time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC), nil
			},
			ComputeAllocationFunc: func(start time.Time, end time.Time, resolution time.Duration) (*opencost.AllocationSet, error) {
				return &opencost.AllocationSet{
					Allocations: map[string]*opencost.Allocation{
						"test": {
							Properties: &opencost.AllocationProperties{
								Namespace:      "test-namespace",
								Controller:     "test-controller-name",
								ControllerKind: "test-controller-kind",
								Pod:            "test-pod",
								Container:      "test-container",
								Labels: map[string]string{
									"test-label1": "test-value1",
									"test-label2": "test-value2",
								},
							},
						},
					},
				}, nil
			},
		}
		err := UpdateCSV(context.TODO(), storage, model, true, []string{"test-label1", "test-label2"})
		require.NoError(t, err)
		// uploaded a single file with the data
		assert.Len(t, model.ComputeAllocationCalls(), 1)
		assert.Equal(t, `Date,Namespace,ControllerKind,ControllerName,Pod,Container,CPUCoreUsageAverage,CPUCoreRequestAverage,RAMBytesUsageAverage,RAMBytesRequestAverage,NetworkReceiveBytes,NetworkTransferBytes,GPUs,PVBytes,CPUCost,RAMCost,NetworkCost,PVCost,GPUCost,TotalCost,Labels,Label_test-label1,Label_test-label2
2021-01-01,test-namespace,test-controller-kind,test-controller-name,test-pod,test-container,0,0,0,0,0,0,0,0,0,0,0,0,0,0,"{""test-label1"":""test-value1"",""test-label2"":""test-value2""}",test-value1,test-value2
`, string(storage.Data))
	})

	t.Run("merge new data with previous data (with different CSV structure)", func(t *testing.T) {
		storage := &filemanager.InMemoryFile{
			Data: []byte(`Date,Namespace,CPUCoreUsageAverage,CPUCoreRequestAverage,CPUCost,RAMBytesUsageAverage,RAMBytesRequestAverage,RAMCost,Label_app
2021-01-01,test-namespace,0.1,0.2,0.3,0.4,0.5,0.6,app1
`),
		}
		model := &AllocationModelMock{
			DateRangeFunc: func() (time.Time, time.Time, error) {
				return time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC), nil
			},
			ComputeAllocationFunc: func(start time.Time, end time.Time, resolution time.Duration) (*opencost.AllocationSet, error) {
				return &opencost.AllocationSet{
					Allocations: map[string]*opencost.Allocation{
						"test": {
							Properties: &opencost.AllocationProperties{
								Namespace: "test-namespace",
							},
							CPUCost: 1,
						},
					},
				}, nil
			},
		}
		err := UpdateCSV(context.TODO(), storage, model, false, nil)
		require.NoError(t, err)
		// uploaded a single file with the data
		assert.Len(t, model.ComputeAllocationCalls(), 1)
		assert.Len(t, model.ComputeAllocationCalls(), 1)
		// 2021-01-01 is already in the export file, so we only compute for 2021-01-02
		assert.Equal(t, time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC), model.ComputeAllocationCalls()[0].Start)
		assert.Equal(t, time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC), model.ComputeAllocationCalls()[0].End)
		assert.Equal(t, `Date,Namespace,CPUCoreUsageAverage,CPUCoreRequestAverage,CPUCost,RAMBytesUsageAverage,RAMBytesRequestAverage,RAMCost,Label_app,ControllerKind,ControllerName,Pod,Container,NetworkReceiveBytes,NetworkTransferBytes,GPUs,PVBytes,NetworkCost,PVCost,GPUCost,TotalCost
2021-01-01,test-namespace,0.1,0.2,0.3,0.4,0.5,0.6,app1,,,,,,,,,,,,
2021-01-02,test-namespace,0,0,1,0,0,0,,,,,,0,0,0,0,0,0,0,1
`, string(storage.Data))
	})

	t.Run("data already present in export file, export should be skipped", func(t *testing.T) {
		data := `Date,Name,CPUCoreUsageAverage,CPUCoreRequestAverage,CPUCost,RAMBytesUsageAverage,RAMBytesRequestAverage,RAMCost
2021-01-01,test,0.1,0.2,0.3,0.4,0.5,0.6
`
		storage := &filemanager.InMemoryFile{
			Data: []byte(data),
		}
		model := &AllocationModelMock{
			DateRangeFunc: func() (time.Time, time.Time, error) {
				return time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC), nil
			},
		}
		err := UpdateCSV(context.TODO(), storage, model, false, nil)
		require.Equal(t, err, errNoData)
		assert.Equal(t, string(storage.Data), data)
		assert.Len(t, model.ComputeAllocationCalls(), 0)
	})

	t.Run("allocation data is empty", func(t *testing.T) {
		model := &AllocationModelMock{
			ComputeAllocationFunc: func(start time.Time, end time.Time, resolution time.Duration) (*opencost.AllocationSet, error) {
				return &opencost.AllocationSet{
					Allocations: nil,
				}, nil
			},
			DateRangeFunc: func() (time.Time, time.Time, error) {
				return time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC), nil
			},
		}
		storage := &filemanager.InMemoryFile{}
		err := UpdateCSV(context.TODO(), storage, model, false, nil)
		require.Equal(t, err, errNoData)
	})
}
