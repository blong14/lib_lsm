package main

// #cgo CFLAGS: -I/home/blong14/Developer/git/lib_lsm/bin/zig-linux-x86_64-0.13.0/lib
// #cgo CFLAGS: -I/home/blong14/Developer/git/lib_lsm/zig-out/include
// #cgo LDFLAGS: -L/home/blong14/Developer/git/lib_lsm/zig-out/lib -llib_lsm
// #include <lib_lsm.h>
import "C"
import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

    _ "unsafe"
)

func main() {
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	_, cancel := context.WithCancel(context.Background())

	s := <-sigint
	log.Printf("received %s signal\n", s)
	cancel()
	time.Sleep(500 * time.Millisecond)
}

