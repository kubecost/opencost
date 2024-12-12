package util

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"reflect"
	"unsafe"

	"github.com/opencost/opencost/core/pkg/util/stringutil"
)

// NonPrimitiveTypeError represents an error where the user provided a non-primitive data type for reading/writing
var NonPrimitiveTypeError error = errors.New("Type provided to read/write does not fit inside 8 bytes.")

// Buffer is a utility type which implements a very basic binary protocol for
// writing core go types.
type Buffer struct {
	b *bytes.Buffer
}

// NewBuffer creates a new Buffer instance using LittleEndian ByteOrder.
func NewBuffer() *Buffer {
	var b bytes.Buffer
	return &Buffer{
		b: &b,
	}
}

// NewBufferFromBytes creates a new Buffer instance using the provided byte slice.
// The new buffer assumes ownership of the byte slice.
func NewBufferFromBytes(b []byte) *Buffer {
	return &Buffer{
		b: bytes.NewBuffer(b),
	}
}

// NewBufferFrom creates a new Buffer instance using the remaining unread data from the
// provided Buffer instance. The new buffer assumes ownership of the underlying data.
func NewBufferFrom(b *Buffer) *Buffer {
	bb := b.Bytes()
	return &Buffer{
		b: bytes.NewBuffer(bb),
	}
}

// WriteBool writes a bool value to the buffer.
func (b *Buffer) WriteBool(t bool) {
	write(b.b, t)
}

// WriteInt writes an int value to the buffer.
func (b *Buffer) WriteInt(i int) {
	write(b.b, int32(i))
}

// WriteInt8 writes an int8 value to the buffer.
func (b *Buffer) WriteInt8(i int8) {
	write(b.b, i)
}

// WriteInt16 writes an int16 value to the buffer.
func (b *Buffer) WriteInt16(i int16) {
	write(b.b, i)
}

// WriteInt32 writes an int32 value to the buffer.
func (b *Buffer) WriteInt32(i int32) {
	write(b.b, i)
}

// WriteInt64 writes an int64 value to the buffer.
func (b *Buffer) WriteInt64(i int64) {
	write(b.b, i)
}

// WriteUInt writes a uint value to the buffer.
func (b *Buffer) WriteUInt(i uint) {
	write(b.b, i)
}

// WriteUInt8 writes a uint8 value to the buffer.
func (b *Buffer) WriteUInt8(i uint8) {
	write(b.b, i)
}

// WriteUInt16 writes a uint16 value to the buffer.
func (b *Buffer) WriteUInt16(i uint16) {
	write(b.b, i)
}

// WriteUInt32 writes a uint32 value to the buffer.
func (b *Buffer) WriteUInt32(i uint32) {
	write(b.b, i)
}

// WriteUInt64 writes a uint64 value to the buffer.
func (b *Buffer) WriteUInt64(i uint64) {
	write(b.b, i)
}

// WriteFloat32 writes a float32 value to the buffer.
func (b *Buffer) WriteFloat32(i float32) {
	write(b.b, i)
}

// WriteFloat64 writes a float64 value to the buffer.
func (b *Buffer) WriteFloat64(i float64) {
	write(b.b, i)
}

// WriteString writes the string's length as a uint16 followed by the string contents.
func (b *Buffer) WriteString(i string) {
	s := stringToBytes(i)
	// string lengths are limited to uint16 - See ReadString()
	if len(s) > math.MaxUint16 {
		s = s[:math.MaxUint16]
	}
	write(b.b, uint16(len(s)))
	b.b.Write(s)
}

// WriteBytes writes the contents of the byte slice to the buffer.
func (b *Buffer) WriteBytes(bytes []byte) {
	b.b.Write(bytes)
}

// ReadBool reads a bool value from the buffer.
func (b *Buffer) ReadBool() bool {
	var i bool
	read(b.b, &i)
	return i
}

// ReadInt reads an int value from the buffer.
func (b *Buffer) ReadInt() int {
	var i int32
	read(b.b, &i)
	return int(i)
}

// ReadInt8 reads an int8 value from the buffer.
func (b *Buffer) ReadInt8() int8 {
	var i int8
	read(b.b, &i)
	return i
}

// ReadInt16 reads an int16 value from the buffer.
func (b *Buffer) ReadInt16() int16 {
	var i int16
	read(b.b, &i)
	return i
}

// ReadInt32 reads an int32 value from the buffer.
func (b *Buffer) ReadInt32() int32 {
	var i int32
	read(b.b, &i)
	return i
}

// ReadInt64 reads an int64 value from the buffer.
func (b *Buffer) ReadInt64() int64 {
	var i int64
	read(b.b, &i)
	return i
}

// ReadUInt reads a uint value from the buffer.
func (b *Buffer) ReadUInt() uint {
	var i uint
	read(b.b, &i)
	return i
}

// ReadUInt8 reads a uint8 value from the buffer.
func (b *Buffer) ReadUInt8() uint8 {
	var i uint8
	read(b.b, &i)
	return i
}

// ReadUInt16 reads a uint16 value from the buffer.
func (b *Buffer) ReadUInt16() uint16 {
	var i uint16
	read(b.b, &i)
	return i
}

// ReadUInt32 reads a uint32 value from the buffer.
func (b *Buffer) ReadUInt32() uint32 {
	var i uint32
	read(b.b, &i)
	return i
}

// ReadUInt64 reads a uint64 value from the buffer.
func (b *Buffer) ReadUInt64() uint64 {
	var i uint64
	read(b.b, &i)
	return i
}

// ReadFloat32 reads a float32 value from the buffer.
func (b *Buffer) ReadFloat32() float32 {
	var i float32
	read(b.b, &i)
	return i
}

// ReadFloat64 reads a float64 value from the buffer.
func (b *Buffer) ReadFloat64() float64 {
	var i float64
	read(b.b, &i)
	return i
}

// ReadString reads a uint16 value from the buffer representing the string's length,
// then uses the length to extract the exact length []byte representing the string.
func (b *Buffer) ReadString() string {
	var l uint16
	read(b.b, &l)
	return bytesToString(b.b.Next(int(l)))
}

// ReadBytes reads the specified length from the buffer and returns the byte slice.
func (b *Buffer) ReadBytes(length int) []byte {
	return b.b.Next(length)
}

// Bytes returns the unread portion of the underlying buffer storage.
func (b *Buffer) Bytes() []byte {
	return b.b.Bytes()
}

// Read reads structured binary data from r into data.
func read(r *bytes.Buffer, data interface{}) error {
	order := binary.LittleEndian

	var b [8]byte
	if n := intDataSize(data); n != 0 {
		bs := b[:n]

		if _, err := readFull(r, bs); err != nil {
			return err
		}

		switch data := data.(type) {
		case *bool:
			*data = bs[0] != 0
		case *int8:
			*data = int8(bs[0])
		case *uint8:
			*data = bs[0]
		case *int16:
			*data = int16(order.Uint16(bs))
		case *uint16:
			*data = order.Uint16(bs)
		case *int32:
			*data = int32(order.Uint32(bs))
		case *uint32:
			*data = order.Uint32(bs)
		case *int64:
			*data = int64(order.Uint64(bs))
		case *uint64:
			*data = order.Uint64(bs)
		case *float32:
			*data = math.Float32frombits(order.Uint32(bs))
		case *float64:
			*data = math.Float64frombits(order.Uint64(bs))
		default:
			n = 0 // fast path doesn't apply
		}

		if n != 0 {
			return nil
		}
	}

	return NonPrimitiveTypeError
}

// read full is a bytes.Buffer specific implementation of ioutil.ReadFull() which
// avoids escaping our stack allocated scratch bytes
func readFull(r *bytes.Buffer, buf []byte) (n int, err error) {
	min := len(buf)
	for n < min && err == nil {
		var nn int
		nn, err = r.Read(buf[n:])
		n += nn
	}
	if n >= min {
		err = nil
	} else if n > 0 && err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return
}

// Write writes the binary representation of data into w.
func write(w *bytes.Buffer, data interface{}) error {
	order := binary.LittleEndian

	var b [8]byte
	if n := intDataSize(data); n != 0 {
		bs := b[:n]

		switch v := data.(type) {
		case *bool:
			if *v {
				bs[0] = 1
			} else {
				bs[0] = 0
			}
		case bool:
			if v {
				bs[0] = 1
			} else {
				bs[0] = 0
			}
		case *int8:
			bs[0] = byte(*v)
		case int8:
			bs[0] = byte(v)
		case *uint8:
			bs[0] = *v
		case uint8:
			bs[0] = v
		case *int16:
			order.PutUint16(bs, uint16(*v))
		case int16:
			order.PutUint16(bs, uint16(v))
		case *uint16:
			order.PutUint16(bs, *v)
		case uint16:
			order.PutUint16(bs, v)
		case *int32:
			order.PutUint32(bs, uint32(*v))
		case int32:
			order.PutUint32(bs, uint32(v))
		case *uint32:
			order.PutUint32(bs, *v)
		case uint32:
			order.PutUint32(bs, v)
		case *int64:
			order.PutUint64(bs, uint64(*v))
		case int64:
			order.PutUint64(bs, uint64(v))
		case *uint64:
			order.PutUint64(bs, *v)
		case uint64:
			order.PutUint64(bs, v)
		case *float32:
			order.PutUint32(bs, math.Float32bits(*v))
		case float32:
			order.PutUint32(bs, math.Float32bits(v))
		case *float64:
			order.PutUint64(bs, math.Float64bits(*v))
		case float64:
			order.PutUint64(bs, math.Float64bits(v))
		}

		_, err := w.Write(bs)
		return err
	}

	return NonPrimitiveTypeError
}

// intDataSize returns the size of the data required to represent the data when encoded.
// It returns zero if the type cannot be implemented by the fast path in Read or Write.
func intDataSize(data interface{}) int {
	switch data.(type) {
	case bool, int8, uint8, *bool, *int8, *uint8:
		return 1
	case int16, uint16, *int16, *uint16:
		return 2
	case int32, uint32, *int32, *uint32:
		return 4
	case int64, uint64, *int64, *uint64:
		return 8
	case float32, *float32:
		return 4
	case float64, *float64:
		return 8
	}
	return 0
}

// Conversion from byte slice to string
func bytesToString(b []byte) string {
	// This code will take the passed byte slice and cast it in-place into a string. By doing
	// this, we are pinning the byte slice's underlying array in memory, preventing it from
	// being garbage collected while the string is still in use. If we are using the Bank()
	// functionality to cache new strings, we risk keeping the pinned array alive. To avoid this,
	// we will use the BankFunc() call which uses the casted string to check for existence of a
	// cached string. If it exists, then we drop the pinned reference immediately and use the
	// cached string. If it does _not_ exist, then we use the passed func() string to allocate a new
	// string and cache it. This will prevent us from allocating throw-away strings just to
	// check our cache.
	pinned := *(*string)(unsafe.Pointer(&b))

	return stringutil.BankFunc(pinned, func() string {
		return string(b)
	})
}

// Direct string to byte conversion that doesn't allocate.
func stringToBytes(s string) (b []byte) {
	strh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh.Data = strh.Data
	sh.Len = strh.Len
	sh.Cap = strh.Len
	return b
}
