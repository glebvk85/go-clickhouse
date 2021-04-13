package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
)

type dataReader interface {
	Read() (record []string, err error)
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
	tsv      dataReader
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

	// skip row before WITH TOTALS,
	// not but do not skip an empty line if it is part of the result
	var canSkip bool
	if len(row) == 1 && row[0] == "" {
		if len(dest) == 1 {
			canSkip = true
		} else {
			return r.Next(dest)
		}
	}

	for i, s := range row {
		reader := strings.NewReader(s)
		v, err := r.parsers[i].Parse(reader)
		if err != nil {
			if canSkip {
				return r.Next(dest)
			}
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
