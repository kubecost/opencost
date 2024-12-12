package costmodel

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/filemanager"
)

type AllocationModel interface {
	ComputeAllocation(start, end time.Time, resolution time.Duration) (*opencost.AllocationSet, error)
	DateRange() (time.Time, time.Time, error)
}

var errNoData = errors.New("no data")

func UpdateCSV(ctx context.Context, fileManager filemanager.FileManager, model AllocationModel, labelsAll bool, labels []string) error {
	exporter := &csvExporter{
		FileManager: fileManager,
		Model:       model,
		LabelsAll:   labelsAll,
		Labels:      labels,
	}
	return exporter.Update(ctx)
}

type csvExporter struct {
	FileManager filemanager.FileManager
	Model       AllocationModel
	Labels      []string // If not empty, create a column for each label prefixed with "Label_"
	LabelsAll   bool     // if true, export all labels to a "Labels" column in JSON format
}

// Update updates CSV file in cloud storage with new allocation data
func (e *csvExporter) Update(ctx context.Context) error {
	allocationDates, err := e.availableAllocationDates()
	if err != nil {
		return err
	}

	if len(allocationDates) == 0 {
		return errors.New("no data to export from prometheus")
	}

	resultTmp, err := os.CreateTemp("", "opencost-export-*.csv")
	if err != nil {
		return err
	}
	defer closeAndDelete(resultTmp)

	previousExportTmp, err := os.CreateTemp("", "opencost-previous-export-*.csv")
	if err != nil {
		return err
	}
	defer closeAndDelete(previousExportTmp)

	err = e.FileManager.Download(ctx, previousExportTmp)
	switch {
	case errors.Is(err, filemanager.ErrNotFound):
		// there is no previous file, so we need to create it
		err := e.writeCSVToWriter(ctx, resultTmp, mapTimeToSlice(allocationDates))
		if err != nil {
			return err
		}
	case err != nil:
		return err
	default:
		// existing export file exists
		// scan through it and ignore all dates that are already in the file
		// avoid modifying existing data or producing duplicates
		err := e.updateExportCSV(ctx, previousExportTmp, allocationDates, resultTmp)
		if err != nil {
			return err
		}
	}

	// we just wrote to the file, so we need to seek to the beginning, so we can read from it
	_, err = resultTmp.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	err = e.FileManager.Upload(ctx, resultTmp)
	if err != nil {
		return err
	}

	log.Info("CSV export updated")

	return nil
}

func (e *csvExporter) updateExportCSV(ctx context.Context, previousExportTmp *os.File, allocationDates map[time.Time]struct{}, result *os.File) error {
	previousDates, err := e.loadDates(previousExportTmp)
	if err != nil {
		return err
	}

	for date := range previousDates {
		delete(allocationDates, date)
	}

	if len(allocationDates) == 0 {
		log.Info("export file in cloud storage already contain data for all dates, skipping update")
		return errNoData
	}

	newExportTmp, err := os.CreateTemp("", "opencost-new-export-*.csv")
	if err != nil {
		return err
	}
	defer closeAndDelete(newExportTmp)

	err = e.writeCSVToWriter(ctx, newExportTmp, mapTimeToSlice(allocationDates))
	if err != nil {
		return err
	}

	err = mergeCSV([]*os.File{previousExportTmp, newExportTmp}, result)
	if err != nil {
		return err
	}
	return nil
}

func (e *csvExporter) availableAllocationDates() (map[time.Time]struct{}, error) {
	start, end, err := e.Model.DateRange()
	if err != nil {
		return nil, err
	}
	if start != time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC) {
		// start doesn't start from 00:00 UTC, it could be truncated by prometheus retention policy
		// skip incomplete data and begin from the day after, otherwise it may corrupt existing data
		start = time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
	}
	end = time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, time.UTC)
	dates := make(map[time.Time]struct{})
	for date := start; date.Before(end); date = date.AddDate(0, 0, 1) {
		dates[date] = struct{}{}
	}
	if len(dates) == 0 {
		return nil, errNoData
	}
	return dates, nil
}

func (e *csvExporter) writeCSVToWriter(ctx context.Context, w io.Writer, dates []time.Time) error {
	fmtFloat := func(f float64) string {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}

	type rowData struct {
		date  time.Time
		alloc *opencost.Allocation
	}

	type columnDef struct {
		column string
		value  func(data rowData) string
	}

	csvDef := []columnDef{
		{
			column: "Date",
			value: func(data rowData) string {
				return data.date.Format("2006-01-02")
			},
		},
		{
			column: "Namespace",
			value: func(data rowData) string {
				return data.alloc.Properties.Namespace
			},
		},
		{
			column: "ControllerKind",
			value: func(data rowData) string {
				return data.alloc.Properties.ControllerKind
			},
		},
		{
			column: "ControllerName",
			value: func(data rowData) string {
				return data.alloc.Properties.Controller
			},
		},
		{
			column: "Pod",
			value: func(data rowData) string {
				return data.alloc.Properties.Pod
			},
		},
		{
			column: "Container",
			value: func(data rowData) string {
				return data.alloc.Properties.Container
			},
		},
		{
			column: "CPUCoreUsageAverage",
			value: func(data rowData) string {
				return fmtFloat(data.alloc.CPUCoreUsageAverage)
			},
		},
		{
			column: "CPUCoreRequestAverage",
			value: func(data rowData) string {
				return fmtFloat(data.alloc.CPUCoreRequestAverage)
			},
		},
		{
			column: "RAMBytesUsageAverage",
			value: func(data rowData) string {
				return fmtFloat(data.alloc.RAMBytesUsageAverage)
			},
		},
		{
			column: "RAMBytesRequestAverage",
			value: func(data rowData) string {
				return fmtFloat(data.alloc.RAMBytesRequestAverage)
			},
		},
		{
			column: "NetworkReceiveBytes",
			value: func(data rowData) string {
				return fmtFloat(data.alloc.NetworkReceiveBytes)
			},
		},
		{
			column: "NetworkTransferBytes",
			value: func(data rowData) string {
				return fmtFloat(data.alloc.NetworkTransferBytes)
			},
		},
		{
			column: "GPUs",
			value: func(data rowData) string {
				return fmtFloat(data.alloc.GPUs())
			},
		},
		{
			column: "PVBytes",
			value: func(data rowData) string {
				return fmtFloat(data.alloc.PVBytes())
			},
		},
		{
			column: "CPUCost",
			value: func(data rowData) string {
				return fmtFloat(data.alloc.CPUTotalCost())
			},
		},
		{
			column: "RAMCost",
			value: func(data rowData) string {
				return fmtFloat(data.alloc.RAMTotalCost())
			},
		},
		{
			column: "NetworkCost",
			value: func(data rowData) string {
				return fmtFloat(data.alloc.NetworkTotalCost())
			},
		},
		{
			column: "PVCost",
			value: func(data rowData) string {
				return fmtFloat(data.alloc.PVTotalCost())
			},
		},
		{
			column: "GPUCost",
			value: func(data rowData) string {
				return fmtFloat(data.alloc.GPUTotalCost())
			},
		},
		{
			column: "TotalCost",
			value: func(data rowData) string {
				return fmtFloat(data.alloc.TotalCost())
			},
		},
	}
	if e.LabelsAll {
		csvDef = append(csvDef, columnDef{
			column: "Labels",
			value: func(data rowData) string {
				return fmtLabelsCSV(data.alloc.Properties.Labels)
			},
		})
	}
	for i := range e.Labels {
		label := e.Labels[i] // it's important to copy the label name, otherwise all closures will reference the same label
		csvDef = append(csvDef, columnDef{
			column: "Label_" + label,
			value: func(data rowData) string {
				value, _ := data.alloc.Properties.Labels[label]
				return value
			},
		})
	}

	header := make([]string, 0, len(csvDef))
	for _, def := range csvDef {
		header = append(header, def.column)
	}

	csvWriter := csv.NewWriter(w)
	lines := 0
	err := csvWriter.Write(header)
	if err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	log.Infof("writing CSV with header: %v", header)

	for _, date := range dates {
		start := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
		end := start.AddDate(0, 0, 1)
		data, err := e.Model.ComputeAllocation(start, end, 5*time.Minute)
		if err != nil {
			return err
		}
		log.Infof("fetched %d records for %s", len(data.Allocations), date.Format("2006-01-02"))
		for _, alloc := range data.Allocations {
			if err := ctx.Err(); err != nil {
				return err
			}
			row := make([]string, 0, len(csvDef))
			for _, def := range csvDef {
				row = append(row, def.value(rowData{date: date, alloc: alloc}))
			}
			err := csvWriter.Write(row)
			if err != nil {
				return fmt.Errorf("failed to write csv row: %w", err)
			}

			lines++
		}
	}

	if lines == 0 {
		return errNoData
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return err
	}
	log.Infof("exported %d lines", lines)
	return nil
}

func fmtLabelsCSV(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}

	data, err := json.Marshal(labels)
	if err != nil {
		log.Errorf("failed to marshal labels: %s", err)
		return ""
	}
	return string(data)
}

// loadDate scans through CSV export file and extract all dates from "Date" column
func (e *csvExporter) loadDates(csvFile *os.File) (map[time.Time]struct{}, error) {
	_, err := csvFile.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seeking to the beginning of csv file: %w", err)
	}
	csvReader := csv.NewReader(csvFile)
	header, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("reading csv header: %w", err)
	}
	dateColIndex := 0
	for i, col := range header {
		if col == "Date" {
			dateColIndex = i
			break
		}
	}
	dates := make(map[time.Time]struct{})
	for {
		row, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading csv row: %w", err)
		}
		date, err := time.Parse("2006-01-02", row[dateColIndex])
		if err != nil {
			return nil, fmt.Errorf("parsing date: %w", err)
		}
		dates[date] = struct{}{}
	}
	return dates, nil
}

// mergeCSV merges multiple csv files into one.
// Files may have different headers, but the result will have a header that is a union of all headers.
// The main goal here is to allow changing CSV format without breaking or loosing existing data.
func mergeCSV(input []*os.File, output *os.File) error {
	var err error
	headers := make([][]string, 0, len(input))
	csvReaders := make([]*csv.Reader, 0, len(input))

	// first, get information about the result header
	for _, file := range input {
		_, err = file.Seek(0, io.SeekStart)
		if err != nil {
			return fmt.Errorf("seeking to the beginning of csv file: %w", err)
		}
		csvReader := csv.NewReader(file)
		header, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			// ignore empty files
			continue
		}
		if err != nil {
			return fmt.Errorf("reading header of csv file: %w", err)
		}
		headers = append(headers, header)
		csvReaders = append(csvReaders, csvReader)
	}

	mapping, header := combineHeaders(headers)
	csvWriter := csv.NewWriter(output)
	err = csvWriter.Write(mergeHeaders(headers))
	if err != nil {
		return fmt.Errorf("writing header to csv file: %w", err)
	}

	for csvIndex, csvReader := range csvReaders {
		for {
			inputLine, err := csvReader.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return fmt.Errorf("reading csv file line: %w", err)
			}

			outputLine := make([]string, len(header))
			for colIndex := range header {
				destColIndex, ok := mapping[csvIndex][colIndex]
				if !ok {
					continue
				}
				outputLine[destColIndex] = inputLine[colIndex]
			}
			err = csvWriter.Write(outputLine)
			if err != nil {
				return fmt.Errorf("writing line to csv file: %w", err)
			}
		}

	}
	csvWriter.Flush()
	// check for errors from the Flush
	if csvWriter.Error() != nil {
		return fmt.Errorf("flushing csv file: %w", csvWriter.Error())
	}

	return nil
}

func combineHeaders(headers [][]string) ([]map[int]int, []string) {
	result := make([]string, 0)
	indices := make([]map[int]int, len(headers))
	for i, header := range headers {
		indices[i] = make(map[int]int)
		for j, column := range header {
			if !contains(result, column) {
				result = append(result, column)
				indices[i][j] = len(result) - 1
			} else {
				indices[i][j] = indexOf(result, column)
			}
		}
	}
	return indices, result
}

func mergeHeaders(headers [][]string) []string {
	result := make([]string, 0)
	for _, header := range headers {
		for _, column := range header {
			if !contains(result, column) {
				result = append(result, column)
			}
		}
	}
	return result
}

func contains(slice []string, item string) bool {
	for _, element := range slice {
		if element == item {
			return true
		}
	}
	return false
}

func indexOf(slice []string, element string) int {
	for i, e := range slice {
		if e == element {
			return i
		}
	}
	return -1
}

func mapTimeToSlice(data map[time.Time]struct{}) []time.Time {
	result := make([]time.Time, 0, len(data))
	for key := range data {
		result = append(result, key)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Before(result[j])
	})
	return result
}

func closeAndDelete(f *os.File) {
	if err := f.Close(); err != nil {
		log.Errorf("error closing file: %v", err)
	}
	if err := os.Remove(f.Name()); err != nil {
		log.Errorf("error deleting file: %v", err)
	}
}
