package azure

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/cloud"
)

// StorageConnection provides access to Azure Storage
type StorageConnection struct {
	StorageConfiguration
	lock             sync.Mutex
	ConnectionStatus cloud.ConnectionStatus
}

func (sc *StorageConnection) GetStatus() cloud.ConnectionStatus {
	// initialize status if it has not done so; this can happen if the integration is inactive
	if sc.ConnectionStatus.String() == "" {
		sc.ConnectionStatus = cloud.InitialStatus
	}
	return sc.ConnectionStatus
}

func (sc *StorageConnection) Equals(config cloud.Config) bool {
	thatConfig, ok := config.(*StorageConnection)
	if !ok {
		return false
	}

	return sc.StorageConfiguration.Equals(&thatConfig.StorageConfiguration)
}

// getBlobURLTemplate returns the correct BlobUrl for whichever Cloud storage account is specified by the AzureCloud configuration
// defaults to the Public Cloud template
func (sc *StorageConnection) getBlobURLTemplate() string {
	// Use gov cloud blob url if gov is detected in AzureCloud
	if strings.Contains(strings.ToLower(sc.Cloud), "gov") {
		return "https://%s.blob.core.usgovcloudapi.net/%s"
	} else if strings.Contains(strings.ToLower(sc.Cloud), "china") {
		// Use China cloud blob url if china is detected in AzureCloud
		return "https://%s.blob.core.chinacloudapi.cn/%s"
	}
	// default to Public Cloud template
	return "https://%s.blob.core.windows.net/%s"
}

// DownloadBlob downloads the Azure Billing CSV into a byte slice
func (sc *StorageConnection) DownloadBlob(blobName string, client *azblob.Client, ctx context.Context) ([]byte, error) {
	log.Infof("Azure Storage: retrieving blob: %v", blobName)

	downloadResponse, err := client.DownloadStream(ctx, sc.Container, blobName, nil)
	if err != nil {
		return nil, fmt.Errorf("Azure: DownloadBlob: failed to download %w", err)
	}
	// NOTE: automatically retries are performed if the connection fails
	retryReader := downloadResponse.NewRetryReader(ctx, &azblob.RetryReaderOptions{})
	defer retryReader.Close()

	// read the body into a buffer
	downloadedData := bytes.Buffer{}

	_, err = downloadedData.ReadFrom(retryReader)
	if err != nil {
		return nil, fmt.Errorf("Azure: DownloadBlob: failed to read downloaded data %w", err)
	}

	return downloadedData.Bytes(), nil
}

// StreamBlob returns an io.Reader for the given blob which uses a re-usable double buffer approach to stream directly
// from blob storage.
func (sc *StorageConnection) StreamBlob(blobName string, client *azblob.Client) (*StreamReader, error) {
	return NewStreamReader(client, sc.Container, blobName)
}

// DownloadBlobToFile downloads the Azure Billing CSV to a local file
func (sc *StorageConnection) DownloadBlobToFile(localFilePath string, blob container.BlobItem, client *azblob.Client, ctx context.Context) error {
	// Lock to prevent accessing a file which may not be fully downloaded
	sc.lock.Lock()
	defer sc.lock.Unlock()
	blobName := *blob.Name
	// Check if file already exists
	if fileInfo, err := os.Stat(localFilePath); err == nil {
		blobModTime := *blob.Properties.LastModified
		// Check if the blob was last modified before the file was modified, indicating that the
		// file is the most recent version of the blob
		if blobModTime.Before(fileInfo.ModTime()) {
			log.Debugf("CloudCost: Azure: DownloadBlobToFile: file %s is more recent than correspondig blob %s", localFilePath, blobName)
			return nil
		}

	}

	// Create filepath
	dir := filepath.Dir(localFilePath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("CloudCost: Azure: DownloadBlobToFile: failed to create directory %w", err)
	}
	fp, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("CloudCost: Azure: DownloadBlobToFile: failed to create file %w", err)
	}
	defer fp.Close()

	// Download newest Azure Billing CSV to disk

	// Time out to prevent deadlock on download
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	log.Infof("CloudCost: Azure: DownloadBlobToFile: retrieving blob: %v", blobName)
	filesize, err := client.DownloadFile(timeoutCtx, sc.Container, blobName, fp, nil)
	if err != nil {
		// Clean up file from failed download
		err2 := os.Remove(localFilePath)
		if err2 != nil {
			log.Errorf("CloudCost: Azure: DownloadBlobToFile: failed to remove file %s after failed download %s", localFilePath, err2.Error())
		}
		return fmt.Errorf("CloudCost: Azure: DownloadBlobToFile: failed to download %w", err)
	}
	log.Infof("CloudCost: Azure: DownloadBlobToFile: retrieved %v of size %dMB", blobName, filesize/1024/1024)

	return nil
}

// deleteFilesOlderThan7d recursively walks the directory specified and deletes
// files which have not been modified in the last 7 days. Returns a list of
// files deleted.
func (sc *StorageConnection) deleteFilesOlderThan7d(localPath string) ([]string, error) {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	duration := 7 * 24 * time.Hour
	cleaned := []string{}
	errs := []string{}

	if _, err := os.Stat(localPath); err != nil {
		return cleaned, nil // localPath does not exist
	}

	filepath.Walk(localPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			errs = append(errs, err.Error())
			return err
		}

		if time.Since(info.ModTime()) > duration {
			err := os.Remove(path)
			if err != nil {
				errs = append(errs, err.Error())
			}
			cleaned = append(cleaned, path)
		}
		return nil
	})

	if len(errs) == 0 {
		return cleaned, nil
	} else {
		return cleaned, fmt.Errorf("deleteFilesOlderThan7d: %v", errs)
	}
}
