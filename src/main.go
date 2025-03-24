package main

/*
#cgo LDFLAGS: /home/blong14/Developer/git/lib_lsm/zig-out/lib/liblib_lsm.a -L/home/blong14/Developer/git/lib_lsm/zig-out/lib -Wl,-rpath,/home/blong14/Developer/git/lib_lsm/zig-out/lib -lm -lstdc++ -pthread -ldl
*/
import "C"
import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
//	"unsafe"
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

