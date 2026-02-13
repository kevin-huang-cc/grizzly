package grizzly

import "strconv"

type Column interface {
	Name() string
	DType() DType
	Len() int
	IsNull(i int) bool
	ValueString(i int) string
	Filter(mask []bool) Column
	Take(order []int) Column
	Less(i, j int) bool
}

type Int64Column struct {
	name  string
	data  []int64
	valid bitmap
}

type Float64Column struct {
	name  string
	data  []float64
	valid bitmap
}

type BoolColumn struct {
	name  string
	data  []bool
	valid bitmap
}

type Utf8Column struct {
	name  string
	data  []string
	valid bitmap
}

func NewInt64Column(name string, data []int64, valid []bool) *Int64Column {
	v := bitmap{}
	if valid == nil {
		v = newBitmap(len(data), true)
	} else {
		v = newBitmapFromBools(valid)
	}
	return &Int64Column{name: name, data: append([]int64(nil), data...), valid: v}
}

func NewFloat64Column(name string, data []float64, valid []bool) *Float64Column {
	v := bitmap{}
	if valid == nil {
		v = newBitmap(len(data), true)
	} else {
		v = newBitmapFromBools(valid)
	}
	return &Float64Column{name: name, data: append([]float64(nil), data...), valid: v}
}

func NewBoolColumn(name string, data []bool, valid []bool) *BoolColumn {
	v := bitmap{}
	if valid == nil {
		v = newBitmap(len(data), true)
	} else {
		v = newBitmapFromBools(valid)
	}
	return &BoolColumn{name: name, data: append([]bool(nil), data...), valid: v}
}

func NewUtf8Column(name string, data []string, valid []bool) *Utf8Column {
	v := bitmap{}
	if valid == nil {
		v = newBitmap(len(data), true)
	} else {
		v = newBitmapFromBools(valid)
	}
	return &Utf8Column{name: name, data: append([]string(nil), data...), valid: v}
}

func newInt64ColumnOwned(name string, data []int64, valid bitmap) *Int64Column {
	return &Int64Column{name: name, data: data, valid: valid}
}

func newFloat64ColumnOwned(name string, data []float64, valid bitmap) *Float64Column {
	return &Float64Column{name: name, data: data, valid: valid}
}

func newBoolColumnOwned(name string, data []bool, valid bitmap) *BoolColumn {
	return &BoolColumn{name: name, data: data, valid: valid}
}

func newUtf8ColumnOwned(name string, data []string, valid bitmap) *Utf8Column {
	return &Utf8Column{name: name, data: data, valid: valid}
}

func (c *Int64Column) Name() string       { return c.name }
func (c *Int64Column) DType() DType       { return DTypeInt64 }
func (c *Int64Column) Len() int           { return len(c.data) }
func (c *Int64Column) IsNull(i int) bool  { return !c.valid.get(i) }
func (c *Int64Column) Value(i int) int64  { return c.data[i] }
func (c *Int64Column) Less(i, j int) bool { return c.data[i] < c.data[j] }
func (c *Int64Column) ValueString(i int) string {
	if c.IsNull(i) {
		return ""
	}
	return strconv.FormatInt(c.data[i], 10)
}
func (c *Int64Column) Filter(mask []bool) Column {
	n := 0
	for i := range mask {
		if mask[i] {
			n++
		}
	}
	out := make([]int64, n)
	valid := bitmapBuilder{}
	idx := 0
	for i := range c.data {
		if !mask[i] {
			continue
		}
		out[idx] = c.data[i]
		valid.Append(!c.IsNull(i))
		idx++
	}
	return newInt64ColumnOwned(c.name, out, valid.Build())
}
func (c *Int64Column) Take(order []int) Column {
	out := make([]int64, len(order))
	valid := bitmapBuilder{}
	for i := range order {
		row := order[i]
		out[i] = c.data[row]
		valid.Append(!c.IsNull(row))
	}
	return newInt64ColumnOwned(c.name, out, valid.Build())
}

func (c *Float64Column) Name() string        { return c.name }
func (c *Float64Column) DType() DType        { return DTypeFloat64 }
func (c *Float64Column) Len() int            { return len(c.data) }
func (c *Float64Column) IsNull(i int) bool   { return !c.valid.get(i) }
func (c *Float64Column) Value(i int) float64 { return c.data[i] }
func (c *Float64Column) Less(i, j int) bool  { return c.data[i] < c.data[j] }
func (c *Float64Column) ValueString(i int) string {
	if c.IsNull(i) {
		return ""
	}
	return strconv.FormatFloat(c.data[i], 'g', -1, 64)
}
func (c *Float64Column) Filter(mask []bool) Column {
	n := 0
	for i := range mask {
		if mask[i] {
			n++
		}
	}
	out := make([]float64, n)
	valid := bitmapBuilder{}
	idx := 0
	for i := range c.data {
		if !mask[i] {
			continue
		}
		out[idx] = c.data[i]
		valid.Append(!c.IsNull(i))
		idx++
	}
	return newFloat64ColumnOwned(c.name, out, valid.Build())
}
func (c *Float64Column) Take(order []int) Column {
	out := make([]float64, len(order))
	valid := bitmapBuilder{}
	for i := range order {
		row := order[i]
		out[i] = c.data[row]
		valid.Append(!c.IsNull(row))
	}
	return newFloat64ColumnOwned(c.name, out, valid.Build())
}

func (c *BoolColumn) Name() string       { return c.name }
func (c *BoolColumn) DType() DType       { return DTypeBool }
func (c *BoolColumn) Len() int           { return len(c.data) }
func (c *BoolColumn) IsNull(i int) bool  { return !c.valid.get(i) }
func (c *BoolColumn) Value(i int) bool   { return c.data[i] }
func (c *BoolColumn) Less(i, j int) bool { return !c.data[i] && c.data[j] }
func (c *BoolColumn) ValueString(i int) string {
	if c.IsNull(i) {
		return ""
	}
	if c.data[i] {
		return "true"
	}
	return "false"
}
func (c *BoolColumn) Filter(mask []bool) Column {
	n := 0
	for i := range mask {
		if mask[i] {
			n++
		}
	}
	out := make([]bool, n)
	valid := bitmapBuilder{}
	idx := 0
	for i := range c.data {
		if !mask[i] {
			continue
		}
		out[idx] = c.data[i]
		valid.Append(!c.IsNull(i))
		idx++
	}
	return newBoolColumnOwned(c.name, out, valid.Build())
}
func (c *BoolColumn) Take(order []int) Column {
	out := make([]bool, len(order))
	valid := bitmapBuilder{}
	for i := range order {
		row := order[i]
		out[i] = c.data[row]
		valid.Append(!c.IsNull(row))
	}
	return newBoolColumnOwned(c.name, out, valid.Build())
}

func (c *Utf8Column) Name() string       { return c.name }
func (c *Utf8Column) DType() DType       { return DTypeUtf8 }
func (c *Utf8Column) Len() int           { return len(c.data) }
func (c *Utf8Column) IsNull(i int) bool  { return !c.valid.get(i) }
func (c *Utf8Column) Value(i int) string { return c.data[i] }
func (c *Utf8Column) Less(i, j int) bool { return c.data[i] < c.data[j] }
func (c *Utf8Column) ValueString(i int) string {
	if c.IsNull(i) {
		return ""
	}
	return c.data[i]
}
func (c *Utf8Column) Filter(mask []bool) Column {
	n := 0
	for i := range mask {
		if mask[i] {
			n++
		}
	}
	out := make([]string, n)
	valid := bitmapBuilder{}
	idx := 0
	for i := range c.data {
		if !mask[i] {
			continue
		}
		out[idx] = c.data[i]
		valid.Append(!c.IsNull(i))
		idx++
	}
	return newUtf8ColumnOwned(c.name, out, valid.Build())
}
func (c *Utf8Column) Take(order []int) Column {
	out := make([]string, len(order))
	valid := bitmapBuilder{}
	for i := range order {
		row := order[i]
		out[i] = c.data[row]
		valid.Append(!c.IsNull(row))
	}
	return newUtf8ColumnOwned(c.name, out, valid.Build())
}
