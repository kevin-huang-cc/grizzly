package grizzly

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

const csvTypeSampleRows = 8192

type typedBuilder interface {
	Append(raw string, row int) error
	Build(name string) Column
}

type int64Builder struct {
	data  []int64
	valid bitmapBuilder
	nulls map[string]struct{}
}

type float64Builder struct {
	data  []float64
	valid bitmapBuilder
	nulls map[string]struct{}
}

type boolBuilder struct {
	data  []bool
	valid bitmapBuilder
	nulls map[string]struct{}
}

type utf8Builder struct {
	data  []string
	valid bitmapBuilder
	nulls map[string]struct{}
}

func readCSV(path string, opts ScanOptions) (*DataFrame, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.ReuseRecord = true
	if opts.Delimiter != 0 {
		r.Comma = opts.Delimiter
	}

	header, err := r.Read()
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("empty csv")
		}
		return nil, err
	}
	if len(header) == 0 {
		return nil, fmt.Errorf("empty csv header")
	}
	header = append([]string(nil), header...)

	nulls := make(map[string]struct{}, len(opts.NullValues))
	for i := range opts.NullValues {
		nulls[opts.NullValues[i]] = struct{}{}
	}

	samples := make([][]string, len(header))
	for i := range samples {
		samples[i] = make([]string, 0, csvTypeSampleRows)
	}
	records := make([][]string, 0, csvTypeSampleRows)
	for len(records) < csvTypeSampleRows {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) != len(header) {
			return nil, fmt.Errorf("csv row has %d columns expected %d", len(rec), len(header))
		}
		cp := append([]string(nil), rec...)
		records = append(records, cp)
		for i := range cp {
			samples[i] = append(samples[i], cp[i])
		}
	}

	builders := make([]typedBuilder, len(header))
	for i := range header {
		builders[i] = newBuilder(inferType(samples[i], nulls), nulls)
	}

	row := 1
	for i := range records {
		for j := range records[i] {
			if err := builders[j].Append(records[i][j], row); err != nil {
				return nil, err
			}
		}
		row++
	}

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) != len(header) {
			return nil, fmt.Errorf("csv row has %d columns expected %d", len(rec), len(header))
		}
		for i := range rec {
			if err := builders[i].Append(rec[i], row); err != nil {
				return nil, err
			}
		}
		row++
	}

	cols := make([]Column, len(header))
	for i := range header {
		cols[i] = builders[i].Build(header[i])
	}
	return NewDataFrame(cols...)
}

func newBuilder(dtype DType, nulls map[string]struct{}) typedBuilder {
	switch dtype {
	case DTypeInt64:
		return &int64Builder{data: make([]int64, 0, 16384), nulls: nulls}
	case DTypeFloat64:
		return &float64Builder{data: make([]float64, 0, 16384), nulls: nulls}
	case DTypeBool:
		return &boolBuilder{data: make([]bool, 0, 16384), nulls: nulls}
	default:
		return &utf8Builder{data: make([]string, 0, 16384), nulls: nulls}
	}
}

func (b *int64Builder) Append(raw string, row int) error {
	b.data = append(b.data, 0)
	if _, ok := b.nulls[raw]; ok {
		b.valid.Append(false)
		return nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fmt.Errorf("row %d parse int64: %w", row, err)
	}
	b.data[len(b.data)-1] = v
	b.valid.Append(true)
	return nil
}
func (b *int64Builder) Build(name string) Column {
	return newInt64ColumnOwned(name, b.data, b.valid.Build())
}

func (b *float64Builder) Append(raw string, row int) error {
	b.data = append(b.data, 0)
	if _, ok := b.nulls[raw]; ok {
		b.valid.Append(false)
		return nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return fmt.Errorf("row %d parse float64: %w", row, err)
	}
	b.data[len(b.data)-1] = v
	b.valid.Append(true)
	return nil
}
func (b *float64Builder) Build(name string) Column {
	return newFloat64ColumnOwned(name, b.data, b.valid.Build())
}

func (b *boolBuilder) Append(raw string, row int) error {
	b.data = append(b.data, false)
	if _, ok := b.nulls[raw]; ok {
		b.valid.Append(false)
		return nil
	}
	v, err := strconv.ParseBool(strings.ToLower(raw))
	if err != nil {
		return fmt.Errorf("row %d parse bool: %w", row, err)
	}
	b.data[len(b.data)-1] = v
	b.valid.Append(true)
	return nil
}
func (b *boolBuilder) Build(name string) Column {
	return newBoolColumnOwned(name, b.data, b.valid.Build())
}

func (b *utf8Builder) Append(raw string, _ int) error {
	b.data = append(b.data, raw)
	_, isNull := b.nulls[raw]
	b.valid.Append(!isNull)
	return nil
}
func (b *utf8Builder) Build(name string) Column {
	return newUtf8ColumnOwned(name, b.data, b.valid.Build())
}

func inferType(values []string, nullSet map[string]struct{}) DType {
	allInt := true
	allFloat := true
	allBool := true
	for i := range values {
		if _, ok := nullSet[values[i]]; ok {
			continue
		}
		if _, err := strconv.ParseInt(values[i], 10, 64); err != nil {
			allInt = false
		}
		if _, err := strconv.ParseFloat(values[i], 64); err != nil {
			allFloat = false
		}
		if _, err := strconv.ParseBool(strings.ToLower(values[i])); err != nil {
			allBool = false
		}
		if !allInt && !allFloat && !allBool {
			return DTypeUtf8
		}
	}
	if allInt {
		return DTypeInt64
	}
	if allFloat {
		return DTypeFloat64
	}
	if allBool {
		return DTypeBool
	}
	return DTypeUtf8
}
