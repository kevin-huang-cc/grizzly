package grizzly

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"sort"
	"strconv"
)

type DataFrame struct {
	columns []Column
	index   map[string]int
	nrows   int
}

func NewDataFrame(cols ...Column) (*DataFrame, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("at least one column required")
	}
	n := cols[0].Len()
	idx := make(map[string]int, len(cols))
	for i := range cols {
		if cols[i].Len() != n {
			return nil, fmt.Errorf("column %s has mismatched length", cols[i].Name())
		}
		if _, ok := idx[cols[i].Name()]; ok {
			return nil, fmt.Errorf("duplicate column %s", cols[i].Name())
		}
		idx[cols[i].Name()] = i
	}
	return &DataFrame{columns: cols, index: idx, nrows: n}, nil
}

func (df *DataFrame) Height() int { return df.nrows }
func (df *DataFrame) Width() int  { return len(df.columns) }

func (df *DataFrame) Columns() []Column {
	out := make([]Column, len(df.columns))
	copy(out, df.columns)
	return out
}

func (df *DataFrame) Column(name string) (Column, bool) {
	i, ok := df.index[name]
	if !ok {
		return nil, false
	}
	return df.columns[i], true
}

func (df *DataFrame) Select(names ...string) (*DataFrame, error) {
	cols := make([]Column, 0, len(names))
	for _, name := range names {
		c, ok := df.Column(name)
		if !ok {
			return nil, fmt.Errorf("unknown column %s", name)
		}
		cols = append(cols, c)
	}
	return NewDataFrame(cols...)
}

func (df *DataFrame) Filter(expr Expr) (*DataFrame, error) {
	mask, err := expr.Eval(df)
	if err != nil {
		return nil, err
	}
	if mask.Len() != df.nrows {
		return nil, fmt.Errorf("mask length mismatch")
	}
	vals := mask.data
	cols := make([]Column, len(df.columns))
	for i := range df.columns {
		cols[i] = df.columns[i].Filter(vals)
	}
	return NewDataFrame(cols...)
}

func (df *DataFrame) SortBy(column string, desc bool) (*DataFrame, error) {
	c, ok := df.Column(column)
	if !ok {
		return nil, fmt.Errorf("unknown column %s", column)
	}
	order := make([]int, df.nrows)
	for i := range order {
		order[i] = i
	}
	switch col := c.(type) {
	case *Int64Column:
		sort.SliceStable(order, func(i, j int) bool {
			li := order[i]
			lj := order[j]
			aiNull := col.IsNull(li)
			ajNull := col.IsNull(lj)
			if aiNull && ajNull {
				return li < lj
			}
			if aiNull {
				return false
			}
			if ajNull {
				return true
			}
			if desc {
				return col.data[li] > col.data[lj]
			}
			return col.data[li] < col.data[lj]
		})
	case *Float64Column:
		sort.SliceStable(order, func(i, j int) bool {
			li := order[i]
			lj := order[j]
			aiNull := col.IsNull(li)
			ajNull := col.IsNull(lj)
			if aiNull && ajNull {
				return li < lj
			}
			if aiNull {
				return false
			}
			if ajNull {
				return true
			}
			if desc {
				return col.data[li] > col.data[lj]
			}
			return col.data[li] < col.data[lj]
		})
	case *BoolColumn:
		sort.SliceStable(order, func(i, j int) bool {
			li := order[i]
			lj := order[j]
			aiNull := col.IsNull(li)
			ajNull := col.IsNull(lj)
			if aiNull && ajNull {
				return li < lj
			}
			if aiNull {
				return false
			}
			if ajNull {
				return true
			}
			if desc {
				return col.data[li] && !col.data[lj]
			}
			return !col.data[li] && col.data[lj]
		})
	case *Utf8Column:
		sort.SliceStable(order, func(i, j int) bool {
			li := order[i]
			lj := order[j]
			aiNull := col.IsNull(li)
			ajNull := col.IsNull(lj)
			if aiNull && ajNull {
				return li < lj
			}
			if aiNull {
				return false
			}
			if ajNull {
				return true
			}
			if desc {
				return col.data[li] > col.data[lj]
			}
			return col.data[li] < col.data[lj]
		})
	default:
		sort.SliceStable(order, func(i, j int) bool {
			li := order[i]
			lj := order[j]
			aiNull := c.IsNull(li)
			ajNull := c.IsNull(lj)
			if aiNull && ajNull {
				return li < lj
			}
			if aiNull {
				return false
			}
			if ajNull {
				return true
			}
			if desc {
				return c.Less(lj, li)
			}
			return c.Less(li, lj)
		})
	}
	cols := make([]Column, len(df.columns))
	for i := range df.columns {
		cols[i] = df.columns[i].Take(order)
	}
	return NewDataFrame(cols...)
}

func (df *DataFrame) ProjectionChecksum(maxCols int) string {
	if maxCols <= 0 {
		maxCols = 1
	}
	if maxCols > len(df.columns) {
		maxCols = len(df.columns)
	}
	h := sha256.New()
	for i := 0; i < df.nrows; i++ {
		for j := 0; j < maxCols; j++ {
			if j > 0 {
				hashWriteByte(h, 0x1f)
			}
			hashWriteString(h, df.columns[j].ValueString(i))
		}
		hashWriteByte(h, '\n')
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (df *DataFrame) MarshalRowsJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i := 0; i < df.nrows; i++ {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('{')
		for j, c := range df.columns {
			if j > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(strconv.Quote(c.Name()))
			buf.WriteByte(':')
			if c.IsNull(i) {
				buf.WriteString("null")
				continue
			}
			switch cc := c.(type) {
			case *Int64Column:
				buf.WriteString(strconv.FormatInt(cc.Value(i), 10))
			case *Float64Column:
				buf.WriteString(strconv.FormatFloat(cc.Value(i), 'g', -1, 64))
			case *BoolColumn:
				if cc.Value(i) {
					buf.WriteString("true")
				} else {
					buf.WriteString("false")
				}
			case *Utf8Column:
				buf.WriteString(strconv.Quote(cc.Value(i)))
			default:
				buf.WriteString(strconv.Quote(c.ValueString(i)))
			}
		}
		buf.WriteByte('}')
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

func hashWriteString(h hash.Hash, s string) {
	_, _ = io.WriteString(h, s)
}

func hashWriteByte(h hash.Hash, b byte) {
	var one [1]byte
	one[0] = b
	_, _ = h.Write(one[:])
}
