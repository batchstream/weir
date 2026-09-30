package main

import (
	"fmt"
	"os"

	"github.com/batchstream/weir/internal/luaengine"
)

func main() {
	if len(os.Args) != 2 || os.Args[1] != "--weir-lua-worker" {
		fmt.Fprintln(os.Stderr, "internal Lua worker invocation required")
		os.Exit(2)
	}
	if err := luaengine.Serve(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "Lua worker failed")
		os.Exit(1)
	}
}
