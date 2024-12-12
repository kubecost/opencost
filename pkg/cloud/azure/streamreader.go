package azure

import (
	"bytes"
	"context"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

const defaultBlockSize = int(8 * 1024 * 1024) // 8MB

// StreamReader is a double buffered streaming reader for Azure Blob Storage.
type StreamReader struct {
	client    *azblob.Client
	container string
	blobName  string
	block     *bytes.Buffer
	next      *streamingBlock
	position  int64
	size      int64
}

// NewStreamReader creates a new streaming reader for the specified blob.
func NewStreamReader(client *azblob.Client, container string, blobName string) (*StreamReader, error) {
	sar := &StreamReader{
		client:    client,
		container: container,
		blobName:  blobName,
		block:     nil,
		next:      nil,
		position:  0,
		size:      0,
	}

	// get the size of the blob
	blobClient := client.ServiceClient().NewContainerClient(container).NewBlobClient(blobName)
	gr, err := blobClient.GetProperties(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	sar.size = *gr.ContentLength

	return sar, nil
}

// See io.Reader.Read
func (r *StreamReader) Read(p []byte) (n int, err error) {
	if r.position >= r.size {
		return 0, io.EOF
	}

	// fetch the blocks on demand
	if r.block == nil || r.block.Len() == 0 {
		err := r.nextBlock()
		if err != nil {
			return 0, err
		}
	}

	// block.Next() constrains the bytes read even if len(p) is larger
	// than the rest of the block
	copied := copy(p, r.block.Next(len(p)))
	r.position += int64(copied)

	return copied, nil
}

// nextBlock fetches the next block of data from the blob and starts the download of
// the next block in the background.
func (r *StreamReader) nextBlock() error {
	// if we don't have a block, we need to fetch the first block, and start fetching
	// the next block in the background
	if r.block == nil {
		current := newStreamBlock(
			r.client,
			r.container,
			r.blobName,
			nil,
			r.position,
			int64(defaultBlockSize),
			r.size,
		)

		// explicitly wait here for the first block
		err := current.Wait()
		if err != nil {
			return err
		}

		// set the current block and start the next block download
		r.block = current.buffer

		// if the block size capacity was reduced to a value different than the default block size,
		// we can assume there is no more data beyond this block, so we don't need to start the next block
		if current.capacity != int64(defaultBlockSize) {
			return nil
		}

		// start next block stream
		r.next = newStreamBlock(
			r.client,
			r.container,
			r.blobName,
			nil,
			r.position+current.capacity,
			int64(defaultBlockSize),
			r.size,
		)

		return nil
	}

	// we have a block and a next block, so we need to wait for the next block to finish
	// buffering, then we can swap current and next buffers
	err := r.next.Wait()
	if err != nil {
		return err
	}

	// save the current buffer to re-use in the next block and set the current to the next block
	currentBuffer := r.block
	r.block = r.next.buffer

	if r.next.capacity != int64(defaultBlockSize) {
		return nil
	}

	// start next block stream
	r.next = newStreamBlock(
		r.client,
		r.container,
		r.blobName,
		currentBuffer, // recycle the old current buffer as the next buffer
		r.position+int64(defaultBlockSize),
		int64(defaultBlockSize),
		r.size,
	)

	return nil
}

// streamingBlock is a buffered block of data that runs in a separate goroutine
// to allow the next block to download while the current block is being read.
type streamingBlock struct {
	client    *azblob.Client
	container string
	blob      string

	done   chan struct{}
	buffer *bytes.Buffer
	err    error

	start    int64
	capacity int64
}

// newStreamBlock creates a new buffered block of data the down the specific
// range of the blob. While the block download runs in a separate goroutine,
// we will never attempt to access the passed buffer until after the Wait()
// returns. This just ensures that we will never attempt to swap buffers
// mid-download.
func newStreamBlock(
	client *azblob.Client,
	container string,
	blob string,
	buffer *bytes.Buffer,
	start int64,
	capacity int64,
	max int64,
) *streamingBlock {
	sb := &streamingBlock{
		client:    client,
		container: container,
		blob:      blob,
		done:      make(chan struct{}),
		buffer:    buffer,
		start:     start,
		capacity:  capacity,
	}

	// determine if we need to reallocate a new block buffer or if we can re-use the existing storage
	blockSize := capacity
	if start+blockSize > max {
		blockSize = max - start
	}

	// if the provided buffer is nil or the blockSize is different than the provided capacity, we need to reallocate
	// reallocation will likely happen once at the end of the stream
	if sb.buffer == nil || blockSize != capacity {
		sb.buffer = bytes.NewBuffer(make([]byte, 0, blockSize))
		sb.capacity = blockSize
	} else {
		sb.buffer.Reset()
	}

	// start a goroutine to fetch the block of data, close the done channel when the block
	// is fetched or an error occurs
	go func(block *streamingBlock) {
		ctx := context.Background()

		opts := azblob.DownloadStreamOptions{
			Range: azblob.HTTPRange{
				Offset: block.start,
				Count:  block.capacity,
			},
		}

		resp, err := block.client.DownloadStream(ctx, block.container, block.blob, &opts)
		if err != nil {
			block.err = err
			close(block.done)
			return
		}

		retryOpts := &azblob.RetryReaderOptions{
			MaxRetries: 3,
		}

		var body io.ReadCloser = resp.NewRetryReader(ctx, retryOpts)
		_, err = io.Copy(block.buffer, body)
		if err != nil {
			block.err = err
			close(block.done)
			return
		}

		err = body.Close()
		if err != nil {
			block.err = err
			close(block.done)
			return
		}

		close(block.done)
	}(sb)

	return sb
}

// Wait blocks until the block is downloaded and returns any error that occurred.
func (sb *streamingBlock) Wait() error {
	<-sb.done

	return sb.err
}
