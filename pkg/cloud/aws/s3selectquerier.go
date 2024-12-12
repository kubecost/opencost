package aws

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/opencost/opencost/core/pkg/util/stringutil"
	"github.com/opencost/opencost/pkg/cloud"
)

type S3SelectQuerier struct {
	S3Connection
	connectionStatus cloud.ConnectionStatus
}

func (s3sq *S3SelectQuerier) Equals(config cloud.Config) bool {
	thatConfig, ok := config.(*S3SelectQuerier)
	if !ok {
		return false
	}

	return s3sq.S3Connection.Equals(&thatConfig.S3Connection)
}

func (s3sq *S3SelectQuerier) Query(query string, queryKeys []string, cli *s3.Client, fn func(*csv.Reader) error) error {
	for _, queryKey := range queryKeys {
		reader, err2 := s3sq.fetchCSVReader(query, queryKey, cli, s3Types.FileHeaderInfoUse)
		if err2 != nil {
			return err2
		}
		err2 = fn(reader)
		if err2 != nil {
			return err2
		}
	}

	return nil
}

func (s3sq *S3SelectQuerier) GetHeaders(queryKey string, cli *s3.Client) ([]string, error) {
	reader, err := s3sq.fetchCSVReader("SELECT * FROM S3Object LIMIT 1", queryKey, cli, s3Types.FileHeaderInfoNone)
	if err != nil {
		return nil, err
	}
	record, err := reader.Read()
	if err != nil {
		return nil, err
	}
	return record, nil
}

// GetQueryKeys returns a list of s3 object names, where the there are 1 object for each month within the range between
// start and end
func (s3sq *S3SelectQuerier) GetQueryKeys(start, end time.Time, client *s3.Client) ([]string, error) {
	objs, err := s3sq.ListObjects(client)
	if err != nil {
		return nil, err
	}

	monthStrings, err := getMonthStrings(start, end)
	if err != err {
		return nil, err
	}

	var queryKeys []string
	// Find all matching "csv.gz" files per monthString
	for _, monthStr := range monthStrings {
		for _, obj := range objs.Contents {
			if strings.Contains(*obj.Key, monthStr) && strings.HasSuffix(*obj.Key, ".csv.gz") {
				queryKeys = append(queryKeys, *obj.Key)
			}
		}
	}

	if len(queryKeys) == 0 {
		return nil, fmt.Errorf("no CUR files for given time range")
	}

	return queryKeys, nil
}

func (s3sq *S3SelectQuerier) fetchCSVReader(query string, queryKey string, client *s3.Client, fileHeaderInfo s3Types.FileHeaderInfo) (*csv.Reader, error) {
	input := &s3.SelectObjectContentInput{
		Bucket:         aws.String(s3sq.Bucket),
		Key:            aws.String(queryKey),
		Expression:     aws.String(query),
		ExpressionType: s3Types.ExpressionTypeSql,
		InputSerialization: &s3Types.InputSerialization{
			CompressionType: s3Types.CompressionTypeGzip,
			CSV: &s3Types.CSVInput{
				FileHeaderInfo: fileHeaderInfo,
			},
		},
		OutputSerialization: &s3Types.OutputSerialization{
			CSV: &s3Types.CSVOutput{},
		},
	}

	res, err := client.SelectObjectContent(context.TODO(), input)
	if err != nil {
		return nil, err
	}
	resStream := res.GetStream()
	// todo: this needs work
	results, resultWriter := io.Pipe()
	go func() {
		defer resultWriter.Close()
		defer resStream.Close()
		resStream.Events()
		for event := range resStream.Events() {
			switch e := event.(type) {
			case *s3Types.SelectObjectContentEventStreamMemberRecords:
				resultWriter.Write(e.Value.Payload)
			case *s3Types.SelectObjectContentEventStreamMemberEnd:
				break
			}

		}
	}()

	if err := resStream.Err(); err != nil {
		return nil, fmt.Errorf("failed to read from SelectObjectContent EventStream, %v", err)
	}

	return csv.NewReader(results), nil
}

func getMonthStrings(start, end time.Time) ([]string, error) {
	if start.After(end) {
		return []string{}, fmt.Errorf("start date must be before end date")
	}
	if end.After(time.Now()) {
		end = time.Now()
	}
	dateTemplate := "%d%02d01-%d%02d01/"
	// set to first of the month
	currMonth := start.AddDate(0, 0, -start.Day()+1)
	nextMonth := currMonth.AddDate(0, 1, 0)
	monthStr := fmt.Sprintf(dateTemplate, currMonth.Year(), int(currMonth.Month()), nextMonth.Year(), int(nextMonth.Month()))

	// Create string for end condition
	endMonth := end.AddDate(0, 0, -end.Day()+1)
	endNextMonth := endMonth.AddDate(0, 1, 0)
	endStr := fmt.Sprintf(dateTemplate, endMonth.Year(), int(endMonth.Month()), endNextMonth.Year(), int(endNextMonth.Month()))

	var monthStrs []string
	monthStrs = append(monthStrs, monthStr)

	for monthStr != endStr {
		currMonth = nextMonth
		nextMonth = nextMonth.AddDate(0, 1, 0)
		monthStr = fmt.Sprintf(dateTemplate, currMonth.Year(), int(currMonth.Month()), nextMonth.Year(), int(nextMonth.Month()))
		monthStrs = append(monthStrs, monthStr)
	}

	return monthStrs, nil
}

// GetCSVRowValue retrieve value from athena row based on column names and used stringutil.Bank() to prevent duplicate
// allocation of strings
func GetCSVRowValue(row []string, queryColumnIndexes map[string]int, columnName string) string {
	if row == nil {
		return ""
	}
	columnIndex, ok := queryColumnIndexes[columnName]
	if !ok {
		return ""
	}
	return stringutil.Bank(row[columnIndex])
}

// GetCSVRowValueFloat retrieve value from athena row based on column names and convert to float if possible.
func GetCSVRowValueFloat(row []string, queryColumnIndexes map[string]int, columnName string) (float64, error) {
	if row == nil {
		return 0.0, fmt.Errorf("getCSVRowValueFloat: nil row")
	}
	columnIndex, ok := queryColumnIndexes[columnName]
	if !ok {
		return 0.0, fmt.Errorf("getCSVRowValueFloat: missing column index: %s", columnName)
	}
	cost, err := strconv.ParseFloat(row[columnIndex], 64)
	if err != nil {
		return cost, fmt.Errorf("getCSVRowValueFloat: failed to parse %s: '%s': %s", columnName, row[columnIndex], err.Error())
	}
	return cost, nil
}
