package util

import (
	"bytes"
	"math"
	"math/rand"
	"testing"
)

func TestBufferReadWrite(t *testing.T) {
	buf := NewBuffer()

	buf.WriteBool(true)
	buf.WriteInt(42)
	buf.WriteFloat64(3.14)
	buf.WriteString("Testing, 1, 2, 3!")

	readBuf := NewBufferFromBytes(buf.Bytes())

	boolVal := readBuf.ReadBool()
	intVal := readBuf.ReadInt()
	floatVal := readBuf.ReadFloat64()
	stringVal := readBuf.ReadString()

	if boolVal != true {
		t.Errorf("Expected bool value to be true, got %v", boolVal)
	}
	if intVal != 42 {
		t.Errorf("Expected int value to be 42, got %v", intVal)
	}
	if floatVal != 3.14 {
		t.Errorf("Expected float value to be 3.14, got %v", floatVal)
	}
	if stringVal != "Testing, 1, 2, 3!" {
		t.Errorf("Expected string value to be 'Hello, World!', got %v", stringVal)
	}
}

func TestBufferWriteReadBytes(t *testing.T) {
	buf := NewBuffer()

	bytesToWrite := []byte{0x01, 0x02, 0x03, 0x04}
	buf.WriteBytes(bytesToWrite)

	readBuf := NewBufferFromBytes(buf.Bytes())
	readBytes := readBuf.ReadBytes(len(bytesToWrite))

	if !bytes.Equal(readBytes, bytesToWrite) {
		t.Errorf("Expected bytes to be %v, got %v", bytesToWrite, readBytes)
	}
}

func TestBufferWriteReadUInt64(t *testing.T) {
	buf := NewBuffer()

	uint64Val := uint64(1234567890)
	buf.WriteUInt64(uint64Val)

	readBuf := NewBufferFromBytes(buf.Bytes())
	readUInt64 := readBuf.ReadUInt64()

	if readUInt64 != uint64Val {
		t.Errorf("Expected uint64 value to be %v, got %v", uint64Val, readUInt64)
	}
}

func TestBufferWriteReadFloat32(t *testing.T) {
	buf := NewBuffer()

	float32Val := float32(3.14159)
	buf.WriteFloat32(float32Val)

	readBuf := NewBufferFromBytes(buf.Bytes())
	readFloat32 := readBuf.ReadFloat32()

	if readFloat32 != float32Val {
		t.Errorf("Expected float32 value to be %v, got %v", float32Val, readFloat32)
	}
}

func TestBufferWriteReadInt8(t *testing.T) {
	buf := NewBuffer()

	int8Val := int8(-42)
	buf.WriteInt8(int8Val)

	readBuf := NewBufferFromBytes(buf.Bytes())
	readInt8 := readBuf.ReadInt8()

	if readInt8 != int8Val {
		t.Errorf("Expected int8 value to be %v, got %v", int8Val, readInt8)
	}
}

func TestBufferWriteReadUInt16(t *testing.T) {
	buf := NewBuffer()

	uint16Val := uint16(65535)
	buf.WriteUInt16(uint16Val)

	readBuf := NewBufferFromBytes(buf.Bytes())
	readUInt16 := readBuf.ReadUInt16()

	if readUInt16 != uint16Val {
		t.Errorf("Expected uint16 value to be %v, got %v", uint16Val, readUInt16)
	}
}

func TestBufferWriteReadInt32(t *testing.T) {
	buf := NewBuffer()

	int32Val := int32(-1234567890)
	buf.WriteInt32(int32Val)

	readBuf := NewBufferFromBytes(buf.Bytes())
	readInt32 := readBuf.ReadInt32()

	if readInt32 != int32Val {
		t.Errorf("Expected int32 value to be %v, got %v", int32Val, readInt32)
	}
}

func TestBufferWriteReadUInt8(t *testing.T) {
	buf := NewBuffer()

	uint8Val := uint8(255)
	buf.WriteUInt8(uint8Val)

	readBuf := NewBufferFromBytes(buf.Bytes())
	readUInt8 := readBuf.ReadUInt8()

	if readUInt8 != uint8Val {
		t.Errorf("Expected uint8 value to be %v, got %v", uint8Val, readUInt8)
	}
}

func TestBufferWriteReadInt16(t *testing.T) {
	buf := NewBuffer()

	int16Val := int16(-32768)
	buf.WriteInt16(int16Val)

	readBuf := NewBufferFromBytes(buf.Bytes())
	readInt16 := readBuf.ReadInt16()

	if readInt16 != int16Val {
		t.Errorf("Expected int16 value to be %v, got %v", int16Val, readInt16)
	}
}

func TestBufferWriteReadUInt32(t *testing.T) {
	buf := NewBuffer()

	uint32Val := uint32(4294967295)
	buf.WriteUInt32(uint32Val)

	readBuf := NewBufferFromBytes(buf.Bytes())
	readUInt32 := readBuf.ReadUInt32()

	if readUInt32 != uint32Val {
		t.Errorf("Expected uint32 value to be %v, got %v", uint32Val, readUInt32)
	}
}

func TestBufferWriteReadInt64(t *testing.T) {
	buf := NewBuffer()

	int64Val := int64(-9223372036854775808)
	buf.WriteInt64(int64Val)

	readBuf := NewBufferFromBytes(buf.Bytes())
	readInt64 := readBuf.ReadInt64()

	if readInt64 != int64Val {
		t.Errorf("Expected int64 value to be %v, got %v", int64Val, readInt64)
	}
}

func TestBufferBytes(t *testing.T) {
	buf := NewBuffer()

	buf.WriteInt(42)
	buf.WriteFloat64(3.14)

	unreadBytes := buf.Bytes()

	newBuf := NewBufferFromBytes(unreadBytes)

	intVal := newBuf.ReadInt()
	floatVal := newBuf.ReadFloat64()

	if intVal != 42 {
		t.Errorf("Expected int value to be 42, got %v", intVal)
	}
	if floatVal != 3.14 {
		t.Errorf("Expected float value to be 3.14, got %v", floatVal)
	}
}

func TestBufferNewBufferFrom(t *testing.T) {
	buf := NewBuffer()

	buf.WriteInt(42)
	buf.WriteFloat64(3.14)

	newBuf := NewBufferFrom(buf)

	intVal := newBuf.ReadInt()
	floatVal := newBuf.ReadFloat64()

	if intVal != 42 {
		t.Errorf("Expected int value to be 42, got %v", intVal)
	}
	if floatVal != 3.14 {
		t.Errorf("Expected float value to be 3.14, got %v", floatVal)
	}
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func generateRandomString(ln int) string {
	b := make([]byte, ln)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestTooLargeStringTruncate(t *testing.T) {
	normalStr := generateRandomString(100)
	bigStr := generateRandomString(math.MaxUint16 + (math.MaxUint16 / 2))
	expectedBigStrRead := bigStr[:math.MaxUint16]

	otherBigStr := generateRandomString(math.MaxUint16)
	plusOne := generateRandomString(math.MaxUint16 + 1)
	expectedPlusOne := plusOne[:math.MaxUint16]

	buf := NewBuffer()

	buf.WriteInt(42)
	buf.WriteFloat64(3.14)
	buf.WriteString(normalStr)
	buf.WriteString(bigStr)
	buf.WriteString(otherBigStr)
	buf.WriteString(plusOne)

	readBuf := NewBufferFromBytes(buf.Bytes())

	intVal := readBuf.ReadInt()
	floatVal := readBuf.ReadFloat64()
	normalStrRead := readBuf.ReadString()
	bigStrRead := readBuf.ReadString()
	otherBigStrRead := readBuf.ReadString()
	plusOneRead := readBuf.ReadString()

	if intVal != 42 {
		t.Errorf("Expected int value to be 42, got %v", intVal)
	}
	if floatVal != 3.14 {
		t.Errorf("Expected float value to be 3.14, got %v", floatVal)
	}
	if normalStrRead != normalStr {
		t.Errorf("Expected string value to be %v, got %v", normalStr, normalStrRead)
	}
	if bigStrRead != expectedBigStrRead {
		t.Errorf("Expected large string values to be equivalent!")
	}
	if otherBigStrRead != otherBigStr {
		t.Errorf("Expected large string values to be equivalent!")
	}
	if plusOneRead != expectedPlusOne {
		t.Errorf("Expected large string values to be equivalent!")
	}
}
