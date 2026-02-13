# grizzly

`grizzly` is a lightweight columnar dataframe-style library for Go.

It focuses on simple, typed columns with nullable values, sorting, filtering, and CSV/JSON encode-decode.

## Features

- Columnar storage with generic columns
- Nullable values via bitmap validity masks
- Multi-key sorting with sort views
- Row filtering and optional materialization
- CSV read/write (inferred or schema-driven)
- JSON marshal/unmarshal
- Struct/interface columns with optional custom comparator

## Install

After setting your module path in `go.mod` (for example `github.com/you/grizzly`):

```bash
go get github.com/you/grizzly
```

## Quick Example

```go
package main

import (
    "fmt"

    "github.com/you/grizzly"
)

func main() {
    id := grizzly.NewColumn("id", []int{3, 1, 2})
    name := grizzly.NewColumn("name", []string{"zoe", "amy", "bob"})

    df, err := grizzly.NewFrame(id, name)
    if err != nil {
        panic(err)
    }

    _ = df.SortBy(grizzly.SortKey{Name: "name"})
    fmt.Print(df.Head(3))
}
```

## Development

```bash
go test ./...
go test . -run '^$' -bench . -benchmem
```
