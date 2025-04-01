// https://github.com/eatonphil/waterbugdb
package main

// #cgo CFLAGS: -I../zig-out/include
// #cgo LDFLAGS: -L../zig-out/lib -llib_lsm
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
	"syscall"
	"time"
	"unsafe"

	"github.com/google/uuid"

	"github.com/jackc/pgproto3/v2"
	pgquery "github.com/pganalyze/pg_query_go/v2"
)

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
	keyBytes := []byte("tables_" + name)
	key := (*C.uchar)(unsafe.Pointer(C.CString(string(keyBytes))))

	resp := C.lsm_read(pe.db, key)
	if resp == nil {
		return nil, fmt.Errorf("%#v -- key not found %s\n", pe.db, keyBytes)
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

	for _, c := range stmt.TableElts {
		cd := c.GetColumnDef()

		tbl.ColumnNames = append(tbl.ColumnNames, cd.Colname)

		// Names is namespaced. So `INT` is pg_catalog.int4. `BIGINT` is pg_catalog.int8.
		var columnType string
		for _, n := range cd.TypeName.Names {
			if columnType != "" {
				columnType += "."
			}
			columnType += n.GetString_().Str
		}
		tbl.ColumnTypes = append(tbl.ColumnTypes, columnType)
	}

	keyBytes := []byte("tables_" + tbl.Name)
	tableBytes, err := json.Marshal(tbl)
	if err != nil {
		return nil, fmt.Errorf("Could not marshal table key: %s", err)
	}

	key := (*C.uchar)(unsafe.Pointer(C.CString(string(keyBytes))))
	value := (*C.uchar)(unsafe.Pointer(C.CString(string(tableBytes))))

	if ok := C.lsm_write(pe.db, key, value); !ok {
		return nil, fmt.Errorf("Could not write %s to %s", tableBytes, keyBytes)
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
			return nil, fmt.Errorf("Could not marshal row: %s", err)
		}

		key := (*C.uchar)(unsafe.Pointer(C.CString(string(keyBytes))))
		value := (*C.uchar)(unsafe.Pointer(C.CString(string(rowBytes))))

		if ok := C.lsm_write(pe.db, key, value); !ok {
			return nil, fmt.Errorf("Could not write %s to %s", rowBytes, keyBytes)
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
	startKey := (*C.uchar)(unsafe.Pointer(C.CString(string(start))))

	end := []byte("tables_")
	endKey := (*C.uchar)(unsafe.Pointer(C.CString(string(end))))

	iter := C.lsm_scan(pe.db, startKey, endKey)
	defer func() { _ = C.lsm_iter_deinit(iter) }()

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

	for {
		select {
		case <-ctx.Done():
			log.Printf("%s handle exiting...", tag)
			return
		default:
		}
		err := pc.handleMessage(pgc)
		if err != nil {
			log.Printf("%s %s", tag, err)
			return
		}
	}
}

func runPgServer(ctx context.Context, pe *pgEngine, port string) {
	ln, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("%s tcp server exiting...", tag)
			return
		default:
		}
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
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

	pe := newPgEngine(db)

	log.Printf("%s starting pg server @ 0.0.0.0:%s...\n", tag, cfg.pgPort)
	go runPgServer(ctx, pe, cfg.pgPort)

	s := <-sigint
	log.Printf("%s received %s signal\n", tag, s)

	cancel()

	C.lsm_deinit(db)

	time.Sleep(500 * time.Millisecond)
}
