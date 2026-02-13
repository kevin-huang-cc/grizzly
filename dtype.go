package grizzly

type DType uint8

const (
	DTypeInvalid DType = iota
	DTypeInt64
	DTypeFloat64
	DTypeBool
	DTypeUtf8
)

func (d DType) String() string {
	switch d {
	case DTypeInt64:
		return "int64"
	case DTypeFloat64:
		return "float64"
	case DTypeBool:
		return "bool"
	case DTypeUtf8:
		return "utf8"
	default:
		return "invalid"
	}
}
