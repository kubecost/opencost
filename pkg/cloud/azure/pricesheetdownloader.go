package azure

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/profiles/2020-09-01/commerce/mgmt/commerce"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"

	"github.com/opencost/opencost/core/pkg/log"
)

type PriceSheetDownloader struct {
	TenantID         string
	ClientID         string
	ClientSecret     string
	BillingAccount   string
	OfferID          string
	ConvertMeterInfo func(info commerce.MeterInfo) (map[string]*AzurePricing, error)
}

func (d *PriceSheetDownloader) GetPricing(ctx context.Context) (map[string]*AzurePricing, error) {
	log.Infof("requesting pricesheet download link")
	url, err := d.getDownloadURL(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting download URL: %w", err)
	}
	log.Infof("downloading pricesheet from %q", url)
	data, err := d.saveData(ctx, url, "pricesheet")
	if err != nil {
		return nil, fmt.Errorf("saving pricesheet from %q: %w", url, err)
	}
	defer data.Close()

	prices, err := d.readPricesheet(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("reading pricesheet: %w", err)
	}
	log.Infof("loaded %d pricings from pricesheet", len(prices))
	return prices, nil
}

func (d *PriceSheetDownloader) getDownloadURL(ctx context.Context) (string, error) {
	cred, err := azidentity.NewClientSecretCredential(d.TenantID, d.ClientID, d.ClientSecret, nil)
	if err != nil {
		return "", fmt.Errorf("creating credential: %w", err)
	}
	client, err := NewPriceSheetClient(d.BillingAccount, cred, nil)
	if err != nil {
		return "", fmt.Errorf("creating pricesheet client: %w", err)
	}
	poller, err := client.BeginDownloadByBillingPeriod(ctx, currentBillingPeriod())
	if err != nil {
		return "", fmt.Errorf("beginning pricesheet download: %w", err)
	}
	resp, err := poller.PollUntilDone(ctx, &runtime.PollUntilDoneOptions{
		Frequency: 30 * time.Second,
	})
	if err != nil {
		return "", fmt.Errorf("polling for pricesheet: %w", err)
	}
	return resp.Properties.DownloadURL, nil
}

func (d PriceSheetDownloader) saveData(ctx context.Context, url, tempName string) (io.ReadCloser, error) {
	// Download file from URL in response.
	out, err := os.CreateTemp("", tempName)
	if err != nil {
		return nil, fmt.Errorf("creating %s temp file: %w", tempName, err)
	}

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("downloading: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
	}

	if _, err := io.Copy(out, resp.Body); err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	_, err = out.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seeking to start of file: %w", err)
	}

	return &removeOnClose{File: out}, nil
}

type removeOnClose struct {
	*os.File
}

func (r *removeOnClose) Close() error {
	err := r.File.Close()
	if err != nil {
		return err
	}
	return os.Remove(r.Name())
}

func (d *PriceSheetDownloader) readPricesheet(ctx context.Context, data io.Reader) (map[string]*AzurePricing, error) {
	// Avoid double-buffering.
	buf, ok := (data).(*bufio.Reader)
	if !ok {
		buf = bufio.NewReader(data)
	}

	// The CSV file starts with two lines before the header without
	// commas (so different numbers of fields as far as the CSV parser
	// is concerned). Skip them before making the CSV reader so we
	// still get the benefit of the row length checks after the
	// header.
	for i := 0; i < 2; i++ {
		_, err := buf.ReadBytes('\n')
		if err != nil {
			return nil, fmt.Errorf("skipping preamble line %d: %w", i, err)
		}
	}
	reader := csv.NewReader(buf)
	reader.ReuseRecord = true

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}
	if err := checkPricesheetHeader(header); err != nil {
		return nil, err
	}

	units := make(map[string]bool)

	results := make(map[string]*AzurePricing)
	lines := 2
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		lines++
		if err != nil {
			return nil, fmt.Errorf("reading line %d: %w", lines, err)
		}

		// Skip savings plan - we should be reporting based on the
		// consumption price because we don't know whether the user is
		// using a savings plan or over their threshold.
		if row[pricesheetPriceType] == "Savings Plan" || row[pricesheetOfferID] != d.OfferID {
			continue
		}

		// TODO: Creating a meter info for each record will cause a
		// lot of GC churn - is it worth reusing one meter info instead?
		meterInfo, err := makeMeterInfo(row)
		if err != nil {
			log.Warnf("making meter info (line %d): %v", lines, err)
			continue
		}

		pricings, err := d.ConvertMeterInfo(meterInfo)
		if err != nil {
			log.Warnf("converting meter to pricings (line %d): %v", lines, err)
			continue
		}

		if pricings != nil {
			units[*meterInfo.Unit] = true
		}

		for key, pricing := range pricings {
			results[key] = pricing
		}
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no matching pricing from price sheet")
	}

	// Keep track of units seen so we can detect if there are any that
	// need handling.
	allUnits := make([]string, 0, len(units))
	for unit := range units {
		allUnits = append(allUnits, unit)
	}
	sort.Strings(allUnits)
	log.Infof("all units in pricesheet: %s", strings.Join(allUnits, ", "))

	return results, nil
}

func checkPricesheetHeader(header []string) error {
	if len(header) < len(pricesheetCols) {
		return fmt.Errorf("too few header columns: got %d, expected %d", len(header), len(pricesheetCols))
	}
	for col, name := range pricesheetCols {
		if !strings.EqualFold(header[col], name) {
			return fmt.Errorf("unexpected header at col %d %q, expected %q", col, header[col], name)
		}
	}
	return nil
}

func makeMeterInfo(row []string) (commerce.MeterInfo, error) {
	price, err := strconv.ParseFloat(row[pricesheetUnitPrice], 64)
	if err != nil {
		return commerce.MeterInfo{}, fmt.Errorf("parsing unit price: %w", err)
	}
	newPrice, unit := normalisePrice(price, row[pricesheetUnit])
	return commerce.MeterInfo{
		MeterName:        ptr(row[pricesheetMeterName]),
		MeterCategory:    ptr(row[pricesheetMeterCategory]),
		MeterSubCategory: ptr(row[pricesheetMeterSubCategory]),
		Unit:             &unit,
		MeterRegion:      ptr(row[pricesheetMeterRegion]),
		MeterRates:       map[string]*float64{"0": &newPrice},
	}, nil
}

var pricesheetCols = []string{
	"Meter ID",
	"Meter name",
	"Meter category",
	"Meter sub-category",
	"Meter region",
	"Unit",
	"Unit of measure",
	"Part number",
	"Unit price",
	"Currency code",
	"Included quantity",
	"Offer Id",
	"Term",
	"Price type",
}

const (
	pricesheetMeterID          = 0
	pricesheetMeterName        = 1
	pricesheetMeterCategory    = 2
	pricesheetMeterSubCategory = 3
	pricesheetMeterRegion      = 4
	pricesheetUnit             = 5
	pricesheetUnitPrice        = 8
	pricesheetCurrencyCode     = 9
	pricesheetOfferID          = 11
	pricesheetPriceType        = 13
)

func currentBillingPeriod() string {
	return time.Now().Format("200601")
}

func ptr[T any](v T) *T {
	return &v
}

// conversions lists all the units seen from the price sheet for
// prices we're interested in with factors to the corresponding units
// in the rate card.
var conversions = map[string]struct {
	divisor float64
	unit    string
}{
	"1 /Month":       {divisor: 1, unit: "1 /Month"},
	"1 Hour":         {divisor: 1, unit: "1 Hour"},
	"1 PiB/Hour":     {divisor: 1_000_000, unit: "1 GiB/Hour"},
	"10 /Month":      {divisor: 10, unit: "1 /Month"},
	"10 Hours":       {divisor: 10, unit: "1 Hour"},
	"100 /Month":     {divisor: 100, unit: "1 /Month"},
	"100 GB/Month":   {divisor: 100, unit: "1 GB/Month"},
	"100 Hours":      {divisor: 100, unit: "1 Hour"},
	"100 TiB/Hour":   {divisor: 100_000, unit: "1 GiB/Hour"},
	"1000 Hours":     {divisor: 1000, unit: "1 Hour"},
	"10000 Hours":    {divisor: 10_000, unit: "1 Hour"},
	"100000 /Hour":   {divisor: 100_000, unit: "1 /Hour"},
	"1000000 /Hour":  {divisor: 1_000_000, unit: "1 /Hour"},
	"10000000 /Hour": {divisor: 10_000_000, unit: "1 /Hour"},
}

func normalisePrice(price float64, unit string) (float64, string) {
	if conv, ok := conversions[unit]; ok {
		return price / conv.divisor, conv.unit
	}

	return price, unit
}
