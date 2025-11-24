package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Value struct {
	typ  string
	str  string
	num  int
	bulk string
	arr  []Value
	raw  []byte
}

type Writer struct {
	writer io.Writer
}

func NewWriter(wr io.Writer) *Writer {
	return &Writer{
		writer: wr,
	}
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{
		reader: bufio.NewReader(rd),
	}

}

func (r *Resp) readLine() ([]byte, int, error) {
	line, err := r.reader.ReadBytes('\n')
	if err != nil {
		return nil, 0, err
	}

	// must end with \r\n
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, 0, fmt.Errorf("protocol error: bad line ending")
	}

	return line[:len(line)-2], len(line), nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()

	if err != nil {
		return 0, 0, err
	}

	i64, err := strconv.ParseInt(string(line), 10, 64)

	if err != nil {
		return 0, n, err
	}

	return int(i64), n, nil
}

func (r *Resp) Read() (Value, error) {

	_type, err := r.reader.ReadByte()

	if err != nil {
		return Value{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("unknown type %v", string(_type))
		return Value{}, nil
	}

}

func (r *Resp) readArray() (Value, error) {
	//*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
	v := Value{}
	v.typ = "array"

	length, _, err := r.readInteger()

	if err != nil {
		return v, err
	}

	v.arr = make([]Value, 0)

	for i := 0; i < length; i++ {
		value, err := r.Read()
		if err != nil {
			return v, err
		}
		v.arr = append(v.arr, value)

	}
	return v, nil

}

func (r *Resp) readBulk() (Value, error) {
	v := Value{}

	v.typ = "bulk"

	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, len)

	//r.reader.Read(bulk)

	// MUST use ReadFull (Read is NOT guaranteed to return full data)
	_, err = io.ReadFull(r.reader, bulk)
	if err != nil {
		return v, err
	}

	v.bulk = string(bulk)

	// Read the trailing CRLF
	r.readLine()

	return v, nil
}

func (w *Writer) Write(v Value) error {
	if v.typ == "raw" {
		_, err := w.writer.Write(v.raw)
		return err
	}
	var bytes = v.Marshal()

	_, err := w.writer.Write(bytes)

	if err != nil {
		return err
	}

	return nil

}

func (v Value) Marshal() []byte {
	switch v.typ {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshallBulk()
	case "string":
		return v.marshalString()
	case "null":
		return v.marshallNull()
	case "error":
		return v.marshallError()
	default:
		return []byte{}
	}

}

func (v Value) marshalArray() []byte {
	len := len(v.arr)

	var bytes []byte

	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.arr[i].Marshal()...)
	}
	return bytes

}

func (v Value) marshalString() []byte {
	var bytes []byte

	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshallError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshallNull() []byte {
	return []byte("$-1\r\n")
}

func (v Value) marshallBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}
