# grizzly

`grizzly` is a clean-slate, performance-first dataframe engine for Go with a lazy query API inspired by polars.

## Core Design

- Typed columnar memory (`int64`, `float64`, `bool`, `utf8`) with validity bitmaps
- Expression-based filtering (`Col("x").Gt(...)`, `Col("id").Even()`) instead of row callbacks
- Lazy query plans with simple optimization (filter push-up)
- Deterministic projection checksums for correctness verification
- CSV + JSON scanners as pluggable sources

## Quick Example

```go
package main

import (
	"fmt"

	"grizzly"
)

func main() {
	df, err := grizzly.
		ScanCSV("data/customers-500000.csv", grizzly.ScanOptions{}).
		Filter(grizzly.Col("Index").Even()).
		Sort("Customer Id", false).
		Select("Index", "Customer Id", "Email").
		Collect()
	if err != nil {
		panic(err)
	}

	fmt.Println("rows", df.Height(), "checksum", df.ProjectionChecksum(3))
}
```

## Direction

This version intentionally drops legacy API compatibility to focus on a high-performance architecture and provide a strong foundation for:

- chunked columns
- parallel scan/filter/sort kernels
- expression fusion and predicate pushdown
- SIMD-oriented execution paths
