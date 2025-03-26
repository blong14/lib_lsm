package main

// #cgo CFLAGS: -I../include
// #cgo LDFLAGS: -L../zig-out/lib -llib_lsm
// #include <lib_lsm.h>
import "C"
import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
    "unsafe"
)

func main() {
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	_, cancel := context.WithCancel(context.Background())

    db := C.lsm_init()

	key := (*C.uchar)(unsafe.Pointer(C.CString("__key__")))
	value := (*C.uchar)(unsafe.Pointer(C.CString("__value_again__")))

    C.lsm_write(db, key, value)

    resp := C.lsm_read(db, key)
    
	return_value := C.GoString((*C.char)(unsafe.Pointer(resp)))
    
    log.Printf("hello %x db w/ %s\n", db, return_value)

	s := <-sigint
	log.Printf("received %s signal\n", s)
	cancel()
	time.Sleep(500 * time.Millisecond)
}

