package main

// https://github.com/eatonphil/waterbugdb

// #cgo CFLAGS: -I../zig-out/include
// #cgo LDFLAGS: -L../zig-out/lib -llib_lsm
// #include <stdlib.h>
// #include <lib_lsm.h>
import "C"
import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/google/uuid"

	"github.com/jackc/pgproto3/v2"
	pgquery "github.com/pganalyze/pg_query_go/v2"
)

// Pre-allocated byte pools to reduce GC pressure
var (
	keyPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 256)
		},
	}
	valuePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 1024)
		},
	}
)

func getKeyBuffer() []byte {
	return keyPool.Get().([]byte)[:0]
}

func putKeyBuffer(buf []byte) {
	if cap(buf) <= 512 { // Don't pool overly large buffers
		keyPool.Put(buf)
	}
}

func getValueBuffer() []byte {
	return valuePool.Get().([]byte)[:0]
}

func putValueBuffer(buf []byte) {
	if cap(buf) <= 2048 { // Don't pool overly large buffers
		valuePool.Put(buf)
	}
}

type LSM = unsafe.Pointer

type pgEngine struct {
	db LSM
}

func newPgEngine(db LSM) *pgEngine {
	return &pgEngine{db}
}

type tableDefinition struct {
	Name        string
	ColumnNames []string
	ColumnTypes []string
}

func (pe *pgEngine) getTableDefinition(name string) (*tableDefinition, error) {
	keyBuf := getKeyBuffer()
	defer putKeyBuffer(keyBuf)

	keyBuf = append(keyBuf, "tables_"...)
	keyBuf = append(keyBuf, name...)

	cKey := C.CString(string(keyBuf))
	defer C.free(unsafe.Pointer(cKey))
	key := (*C.uchar)(unsafe.Pointer(cKey))

	resp := C.lsm_read(pe.db, key)
	if resp == nil {
		return nil, fmt.Errorf("%#v -- key not found %s", pe.db, keyBuf)
	}

	valBytes := []byte(C.GoString((*C.char)(unsafe.Pointer(resp))))

	var tbl tableDefinition
	err := json.Unmarshal(valBytes, &tbl)
	if err != nil {
		return nil, fmt.Errorf("Could not unmarshal table definition: %s", err)
	}

	return &tbl, err
}

type pgResult struct {
	fieldNames []string
	fieldTypes []string
	rows       [][]any
}

func (pe *pgEngine) executeCreate(stmt *pgquery.CreateStmt) (*pgResult, error) {
	tbl := tableDefinition{}
	tbl.Name = stmt.Relation.Relname

	// Pre-allocate slices with estimated capacity
	tbl.ColumnNames = make([]string, 0, len(stmt.TableElts))
	tbl.ColumnTypes = make([]string, 0, len(stmt.TableElts))

	for _, c := range stmt.TableElts {
		cd := c.GetColumnDef()

		tbl.ColumnNames = append(tbl.ColumnNames, cd.Colname)

		// Use strings.Builder for efficient string concatenation
		var columnType strings.Builder
		for i, n := range cd.TypeName.Names {
			if i > 0 {
				columnType.WriteByte('.')
			}
			columnType.WriteString(n.GetString_().Str)
		}
		tbl.ColumnTypes = append(tbl.ColumnTypes, columnType.String())
	}

	keyBuf := getKeyBuffer()
	defer putKeyBuffer(keyBuf)
	keyBuf = append(keyBuf, "tables_"...)
	keyBuf = append(keyBuf, tbl.Name...)

	tableBytes, err := json.Marshal(tbl)
	if err != nil {
		return nil, fmt.Errorf("could not marshal table key: %s", err)
	}

	cKey := C.CString(string(keyBuf))
	defer C.free(unsafe.Pointer(cKey))
	key := (*C.uchar)(unsafe.Pointer(cKey))

	cValue := C.CString(string(tableBytes))
	defer C.free(unsafe.Pointer(cValue))
	value := (*C.uchar)(unsafe.Pointer(cValue))

	if ok := C.lsm_write(pe.db, key, value); !ok {
		return nil, fmt.Errorf("could not write %s to %s", tableBytes, keyBuf)
	}

	results := &pgResult{}

	return results, nil
}

func (pe *pgEngine) executeInsert(stmt *pgquery.InsertStmt) (*pgResult, error) {
	tblName := stmt.Relation.Relname

	results := &pgResult{}

	slct := stmt.GetSelectStmt().GetSelectStmt()
	for _, values := range slct.ValuesLists {
		var rowData []any
		for _, value := range values.GetList().Items {
			if c := value.GetAConst(); c != nil {
				if s := c.Val.GetString_(); s != nil {
					rowData = append(rowData, s.Str)
					continue
				}

				if i := c.Val.GetInteger(); i != nil {
					rowData = append(rowData, i.Ival)
					continue
				}
			}

			return nil, fmt.Errorf("Unknown value type: %s", value)
		}

		id := uuid.New().String()
		keyBytes := []byte("rows_" + tblName + "_" + id)

		rowBytes, err := json.Marshal(rowData)
		if err != nil {
			return nil, fmt.Errorf("could not marshal row: %s", err)
		}

		cKey := C.CString(string(keyBytes))
		defer C.free(unsafe.Pointer(cKey))
		key := (*C.uchar)(unsafe.Pointer(cKey))

		cValue := C.CString(string(rowBytes))
		defer C.free(unsafe.Pointer(cValue))
		value := (*C.uchar)(unsafe.Pointer(cValue))

		if ok := C.lsm_write(pe.db, key, value); !ok {
			return nil, fmt.Errorf("could not write %s to %s", rowBytes, keyBytes)
		}

		results.rows = append(results.rows, rowData)
	}

	return results, nil
}

func (pe *pgEngine) executeSelect(stmt *pgquery.SelectStmt) (*pgResult, error) {
	tblName := stmt.FromClause[0].GetRangeVar().Relname
	tbl, err := pe.getTableDefinition(tblName)
	if err != nil {
		return nil, err
	}

	results := &pgResult{}
	for _, c := range stmt.TargetList {
		fieldName := c.GetResTarget().Val.GetColumnRef().Fields[0].GetString_().Str
		results.fieldNames = append(results.fieldNames, fieldName)

		fieldType := ""
		for i, cn := range tbl.ColumnNames {
			if cn == fieldName {
				fieldType = tbl.ColumnTypes[i]
			}
		}

		if fieldType == "" {
			return nil, fmt.Errorf("Unknown field: %s", fieldName)
		}

		results.fieldTypes = append(results.fieldTypes, fieldType)
	}

	start := []byte("rows_" + tblName + "_")
	cStartKey := C.CString(string(start))
	defer C.free(unsafe.Pointer(cStartKey))
	startKey := (*C.uchar)(unsafe.Pointer(cStartKey))

	end := []byte("tables_")
	cEndKey := C.CString(string(end))
	defer C.free(unsafe.Pointer(cEndKey))
	endKey := (*C.uchar)(unsafe.Pointer(cEndKey))

	iter := C.lsm_scan(pe.db, startKey, endKey)
	if iter == nil {
		return nil, fmt.Errorf("failed to create iterator")
	}
	defer func() {
		if !C.lsm_iter_deinit(iter) {
			log.Printf("warning: failed to deinitialize iterator")
		}
	}()

	for nxt := C.lsm_iter_next(iter); nxt != nil; nxt = C.lsm_iter_next(iter) {
		var row []any
		rowBytes := []byte(C.GoString((*C.char)(unsafe.Pointer(nxt))))
		err = json.Unmarshal(rowBytes, &row)
		if err != nil {
			return nil, fmt.Errorf("Unable to unmarshal row: %s", err)
		}

		var targetRow []any
		for _, target := range results.fieldNames {
			for i, field := range tbl.ColumnNames {
				if target == field {
					targetRow = append(targetRow, row[i])
				}
			}
		}

		results.rows = append(results.rows, targetRow)
	}

	return results, nil
}

func (pe *pgEngine) execute(tree *pgquery.ParseResult) (*pgResult, error) {
	for _, stmt := range tree.GetStmts() {
		n := stmt.GetStmt()
		if c := n.GetCreateStmt(); c != nil {
			return pe.executeCreate(c)
		}

		if c := n.GetInsertStmt(); c != nil {
			return pe.executeInsert(c)
		}

		if c := n.GetSelectStmt(); c != nil {
			return pe.executeSelect(c)
		}

		return nil, fmt.Errorf("Unknown statement type: %s", stmt)
	}

	return nil, fmt.Errorf("No statements to execute")
}

var dataTypeOIDMap = map[string]uint32{
	"text":            25,
	"pg_catalog.int4": 23,
}

type pgConn struct {
	conn net.Conn
	pe   *pgEngine
}

func (pc pgConn) done(buf []byte, msg string) {
	var err error
	buf, err = (&pgproto3.CommandComplete{CommandTag: []byte(msg)}).Encode(buf)
	if err != nil {
		log.Printf("%s failed to encode buf: %s", tag, err)
		return
	}

	buf, err = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	if err != nil {
		log.Printf("%s failed to encode buf: %s", tag, err)
		return
	}

	pc.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	_, err = pc.conn.Write(buf)
	if err != nil {
		log.Printf("%s failed to write query response: %s", tag, err)
		return
	}
}

func (pc pgConn) writePgResult(res *pgResult) {
	rd := &pgproto3.RowDescription{}
	for i, field := range res.fieldNames {
		rd.Fields = append(rd.Fields, pgproto3.FieldDescription{
			Name:        []byte(field),
			DataTypeOID: dataTypeOIDMap[res.fieldTypes[i]],
		})
	}

	buf, err := rd.Encode(nil)
	if err != nil {
		log.Printf("%s failed to encode buf: %s\n", tag, err)
		return
	}

	for _, row := range res.rows {
		dr := &pgproto3.DataRow{}
		for _, value := range row {
			bs, err := json.Marshal(value)
			if err != nil {
				log.Printf("%s failed to marshal cell: %s\n", tag, err)
				return
			}
			dr.Values = append(dr.Values, bs)
		}
		buf, err = dr.Encode(buf)
		if err != nil {
			log.Printf("%s failed to encode buf: %s\n", tag, err)
			return
		}
	}

	pc.done(buf, fmt.Sprintf("SELECT %d", len(res.rows)))
}

func (pc pgConn) handleStartupMessage(pgconn *pgproto3.Backend) error {
	startupMessage, err := pgconn.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("Error receiving startup message: %s", err)
	}

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf, err := (&pgproto3.AuthenticationOk{}).Encode(nil)
		if err != nil {
			return fmt.Errorf("Error encoding `auth ok` ready for query: %s", err)
		}

		buf, err = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		if err != nil {
			return fmt.Errorf("Error encoding `ready ok` for query: %s", err)
		}

		_, err = pc.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("Error writing ready for query: %s", err)
		}

		return nil
	case *pgproto3.SSLRequest:
		_, err = pc.conn.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("Error sending deny SSL request: %s", err)
		}

		return pc.handleStartupMessage(pgconn)
	default:
		return fmt.Errorf("Unknown startup message: %#v", startupMessage)
	}
}

func (pc pgConn) handleMessage(pgc *pgproto3.Backend) error {
	msg, err := pgc.Receive()
	if err != nil {
		return fmt.Errorf("Error receiving message: %s", err)
	}

	switch t := msg.(type) {
	case *pgproto3.Query:
		stmts, err := pgquery.Parse(t.String)
		if err != nil {
			return fmt.Errorf("Error parsing query: %s", err)
		}

		if len(stmts.GetStmts()) > 1 {
			return fmt.Errorf("Error more than one statement received [count = %d]", len(stmts.GetStmts()))
		}

		res, err := pc.pe.execute(stmts)
		if err != nil {
			return err
		}

		pc.writePgResult(res)

		pc.done(nil, strings.ToUpper(strings.Split(t.String, " ")[0])+" ok")
	case *pgproto3.Terminate:
		return nil
	default:
		return fmt.Errorf("Error received message other than Query from client: %s", msg)
	}

	return nil
}

func (pc pgConn) handle(ctx context.Context) {
	pgc := pgproto3.NewBackend(pgproto3.NewChunkReader(pc.conn), pc.conn)
	defer pc.conn.Close()

	err := pc.handleStartupMessage(pgc)
	if err != nil {
		log.Printf("%s %s", tag, err)
		return
	}

	// Set a reasonable read deadline for each message
	readTimeout := 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			log.Printf("%s handle exiting...", tag)
			return
		default:
			// Set read deadline for this iteration
			if err := pc.conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
				log.Printf("%s failed to set read deadline: %s", tag, err)
				return
			}

			err := pc.handleMessage(pgc)
			if err != nil {
				log.Printf("%s %s", tag, err)
				return
			}
		}
	}
}

func runPgServer(ctx context.Context, pe *pgEngine, port string) {
	ln, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	// When the main context is done, close the listener to unblock Accept
	go func() {
		<-ctx.Done()
		log.Printf("%s tcp server exiting...", tag)
		ln.Close()
	}()

	for {
		// Set accept deadline
		if err := ln.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second)); err != nil {
			log.Printf("%s failed to set accept deadline: %s", tag, err)
		}

		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				// Context was canceled, exit gracefully
				return
			}

			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				// This is just a timeout from our deadline, continue
				continue
			}

			log.Printf("%s accept error: %s", tag, err)
			continue
		}

		pc := pgConn{conn, pe}
		go pc.handle(ctx)
	}
}

type config struct {
	id     string
	pgPort string
}

func getConfig() config {
	cfg := config{}
	for i, arg := range os.Args[1:] {
		if arg == "--pg-port" {
			cfg.pgPort = os.Args[i+2]
			i++
			continue
		}
	}

	if cfg.pgPort == "" {
		cfg.pgPort = "54321"
	}

	return cfg
}

const tag = "[go]"

func main() {
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := getConfig()

	dataDir := "data"
	err := os.MkdirAll(dataDir, os.ModePerm)
	if err != nil {
		log.Fatalf("%s Could not create data directory: %s", tag, err)
	}

	db := C.lsm_init()
	if db == nil {
		log.Fatalf("%s Could not init database", tag)
	}

	// Ensure database is properly cleaned up
	defer func() {
		if !C.lsm_deinit(db) {
			log.Printf("%s warning: failed to properly deinitialize database", tag)
		}
	}()

	pe := newPgEngine(db)

	// Create a wait group to track when server has fully stopped
	var wg sync.WaitGroup
	wg.Add(1)

	log.Printf("%s starting pg server @ localhost:%s", tag, cfg.pgPort)
	go func() {
		defer wg.Done()
		runPgServer(ctx, pe, cfg.pgPort)
	}()

	// Wait for termination signal
	s := <-sigint
	log.Printf("%s received %s signal", tag, s)

	// Trigger graceful shutdown
	cancel()

	// Wait for server to fully stop with a timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("%s server shutdown complete", tag)
	case <-time.After(3 * time.Second):
		log.Printf("%s server shutdown timed out", tag)
	}
}
