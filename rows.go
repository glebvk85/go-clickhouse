package clickhouse

import (
	"bufio"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
)

type TsvParser struct {
    r *bufio.Reader
    rawBuffer []byte
}

type DataReader interface {
    Read() (record []string, err error)
}

func newReader(r io.Reader) *TsvParser {
	return &TsvParser{
		r:     bufio.NewReader(r),
	}
}


func (r *TsvParser) readLine() ([]byte, error) {
	line, err := r.r.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		r.rawBuffer = append(r.rawBuffer[:0], line...)
		for err == bufio.ErrBufferFull {
			line, err = r.r.ReadSlice('\n')
			r.rawBuffer = append(r.rawBuffer, line...)
		}
		line = r.rawBuffer
	}
	if len(line) > 0 && err == io.EOF {
		err = nil
		// For backwards compatibility, drop trailing \r before EOF.
		if line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
	}
	// Normalize \r\n to \n on all input lines.
	if n := len(line); n >= 2 && line[n-2] == '\r' && line[n-1] == '\n' {
		line[n-2] = '\n'
		line = line[:n-1]
	}
	return line, err
}

func (r *TsvParser) Read() (record []string, err error) {
    line, errRead := r.readLine()
    if errRead != nil {
        return nil, errRead
    }
    t := strings.Split(strings.Trim(string(line), "\n"), "\t")
    return t, nil
}

func newTextRows(c *conn, body io.ReadCloser, location *time.Location, useDBLocation bool) (*textRows, error) {
	tsvReader := newReader(body)

	columns, err := tsvReader.Read()
	if err != nil {
		return nil, err
	}

	types, err := tsvReader.Read()
	if err != nil {
		return nil, err
	}
	for i := range types {
		types[i], err = readUnquoted(strings.NewReader(types[i]), 0)
		if err != nil {
			return nil, err
		}
	}

	parsers := make([]DataParser, len(types))
	for i, typ := range types {
		desc, err := ParseTypeDesc(typ)
		if err != nil {
			return nil, err
		}

		parsers[i], err = NewDataParser(desc, &DataParserOptions{
			Location:      location,
			UseDBLocation: useDBLocation,
		})
		if err != nil {
			return nil, err
		}
	}

	return &textRows{
		c:        c,
		respBody: body,
		tsv:      tsvReader,
		columns:  columns,
		types:    types,
		parsers:  parsers,
	}, nil
}

type textRows struct {
	c        *conn
	respBody io.ReadCloser
	tsv      DataReader
	columns  []string
	types    []string
	parsers  []DataParser
}

func (r *textRows) Columns() []string {
	return r.columns
}

func (r *textRows) Close() error {
	r.c.cancel = nil
	return r.respBody.Close()
}

func (r *textRows) Next(dest []driver.Value) error {
	row, err := r.tsv.Read()
	if err != nil {
		return err
	}

	for i, s := range row {
		reader := strings.NewReader(s)
		v, err := r.parsers[i].Parse(reader)
		if err != nil {
			return err
		}
		if _, _, err := reader.ReadRune(); err != io.EOF {
			return fmt.Errorf("trailing data after parsing the value")
		}
		dest[i] = v
	}

	return nil
}

// ColumnTypeScanType implements the driver.RowsColumnTypeScanType
func (r *textRows) ColumnTypeScanType(index int) reflect.Type {
	return r.parsers[index].Type()
}

// ColumnTypeDatabaseTypeName implements the driver.RowsColumnTypeDatabaseTypeName
func (r *textRows) ColumnTypeDatabaseTypeName(index int) string {
	return r.types[index]
}
