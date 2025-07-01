package utils

import (
	"fmt"
	"os"
	"runtime/debug"
)

func Panic(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		stack := debug.Stack()
		fmt.Fprintln(os.Stderr, "Stack trace:")
		os.Stderr.Write(stack)
		os.Exit(1)
	}
}
