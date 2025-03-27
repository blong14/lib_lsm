// https://github.com/eatonphil/waterbugdb
package main

// #cgo CFLAGS: -I../include
// #cgo LDFLAGS: -L../zig-out/lib -llib_lsm
// #include <lib_lsm.h>
import "C"
import (
	"bytes"
    "context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path"
	"syscall"
    "time"
    "unsafe"

	"github.com/google/uuid"

    "github.com/jackc/pgproto3/v2"
	pgquery "github.com/pganalyze/pg_query_go/v2"
)

type pgEngine struct {
	db         *bolt.DB
	bucketName []byte
}

func newPgEngine(db *bolt.DB) *pgEngine {
	return &pgEngine{db, []byte("data")}
}

type tableDefinition struct {
	Name        string
	ColumnNames []string
	ColumnTypes []string
}

func (pe *pgEngine) executeCreate(stmt *pgquery.CreateStmt) error {
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

	tableBytes, err := json.Marshal(tbl)
	if err != nil {
		return fmt.Errorf("Could not marshal table: %s", err)
	}

	err = pe.db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists(pe.bucketName)
		if err != nil {
			return err
		}

		return bkt.Put([]byte("tables_"+tbl.Name), tableBytes)
	})

	if err != nil {
		return fmt.Errorf("Could not set key-value: %s", err)
	}

	return nil
}

func (pe *pgEngine) getTableDefinition(name string) (*tableDefinition, error) {
	var tbl tableDefinition

	err := pe.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pe.bucketName)
		if bkt == nil {
			return fmt.Errorf("Table does not exist")
		}

		valBytes := bkt.Get([]byte("tables_" + name))
		err := json.Unmarshal(valBytes, &tbl)
		if err != nil {
			return fmt.Errorf("Could not unmarshal table: %s", err)
		}

		return nil
	})

	return &tbl, err
}

func (pe *pgEngine) executeInsert(stmt *pgquery.InsertStmt) error {
	tblName := stmt.Relation.Relname

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

			return fmt.Errorf("Unknown value type: %s", value)
		}

		rowBytes, err := json.Marshal(rowData)
		if err != nil {
			return fmt.Errorf("Could not marshal row: %s", err)
		}

		id := uuid.New().String()
		err = pe.db.Update(func(tx *bolt.Tx) error {
			bkt, err := tx.CreateBucketIfNotExists(pe.bucketName)
			if err != nil {
				return err
			}

			return bkt.Put([]byte("rows_"+tblName+"_"+id), rowBytes)
		})
		if err != nil {
			return fmt.Errorf("Could not store row: %s", err)
		}
	}

	return nil
}

type pgResult struct {
	fieldNames []string
	fieldTypes []string
	rows       [][]any
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

	prefix := []byte("rows_" + tblName + "_")
	pe.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(pe.bucketName).Cursor()

		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var row []any
			err = json.Unmarshal(v, &row)
			if err != nil {
				return fmt.Errorf("Unable to unmarshal row: %s", err)
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

		return nil
	})

	return results, nil
}

func (pe *pgEngine) execute(tree *pgquery.ParseResult) error {
	for _, stmt := range tree.GetStmts() {
		n := stmt.GetStmt()
		if c := n.GetCreateStmt(); c != nil {
			return pe.executeCreate(c)
		}

		if c := n.GetInsertStmt(); c != nil {
			return pe.executeInsert(c)
		}

		if c := n.GetSelectStmt(); c != nil {
			_, err := pe.executeSelect(c)
			return err
		}

		return fmt.Errorf("Unknown statement type: %s", stmt)
	}

	return nil
}

func (pe *pgEngine) delete() error {
	return pe.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pe.bucketName)
		if bkt != nil {
			return tx.DeleteBucket(pe.bucketName)
		}

		return nil
	})
}

var dataTypeOIDMap = map[string]uint32{
	"text":            25,
	"pg_catalog.int4": 23,
}

type pgConn struct {
	conn net.Conn
	db   *bolt.DB
}

func (pc pgConn) done(buf []byte, msg string) {
	buf = (&pgproto3.CommandComplete{CommandTag: []byte(msg)}).Encode(buf)
	buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
    pc.conn.SetWriteDeadline(time.Now().Add(3*time.Second))
	_, err := pc.conn.Write(buf)
	if err != nil {
		log.Printf("Failed to write query response: %s", err)
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
	buf := rd.Encode(nil)
	for _, row := range res.rows {
		dr := &pgproto3.DataRow{}
		for _, value := range row {
			bs, err := json.Marshal(value)
			if err != nil {
				log.Printf("Failed to marshal cell: %s\n", err)
				return
			}

			dr.Values = append(dr.Values, bs)
		}

		buf = dr.Encode(buf)
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
		buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
		buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		_, err = pc.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("Error sending ready for query: %s", err)
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
			return fmt.Errorf("Only make one request at a time.")
		}

		stmt := stmts.GetStmts()[0]

		// Handle SELECTs here
		s := stmt.GetStmt().GetSelectStmt()
		if s != nil {
			pe := newPgEngine(pc.db)
			res, err := pe.executeSelect(s)
			if err != nil {
				return err
			}

			pc.writePgResult(res)
			return nil
		}

		pc.done(nil, strings.ToUpper(strings.Split(t.String, " ")[0])+" ok")
	case *pgproto3.Terminate:
		return nil
	default:
		return fmt.Errorf("Received message other than Query from client: %s", msg)
	}

	return nil
}

func (pc pgConn) handle(ctx context.Context) {
	pgc := pgproto3.NewBackend(pgproto3.NewChunkReader(pc.conn), pc.conn)
	defer pc.conn.Close()

	err := pc.handleStartupMessage(pgc)
	if err != nil {
		log.Println(err)
		return
	}

	for {
        select {
        case <-ctx.Done():
            return
        default:
        }
		err := pc.handleMessage(pgc)
		if err != nil {
			log.Println(err)
			return
		}
	}
}

func runPgServer(ctx context.Context, port string, db *bolt.DB) {
	ln, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		log.Fatal(err)
	}

	for {
        select {
        case <-ctx.Done():
            return
        default:
        }
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}

		pc := pgConn{conn, db}
		go pc.handle(ctx)
	}
}

type config struct {
	id       string
	pgPort   string
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
		log.Fatal("Missing required parameter: --pg-port")
	}

	return cfg
}

func main() {
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig()

	dataDir := "data"
	err := os.MkdirAll(dataDir, os.ModePerm)
	if err != nil {
		log.Fatalf("Could not create data directory: %s", err)
	}

	db, err := bolt.Open(path.Join(dataDir, "/data"+cfg.id), 0600, nil)
	if err != nil {
		log.Fatalf("Could not open bolt db: %s", err)
	}
	defer db.Close()
    db := C.lsm_init(dataDir)
    defer C.lsm_deinit(db)

	pe := newPgEngine(db)
	// Start off in clean state
	// pe.delete()

	go runPgServer(ctx, cfg.pgPort, db)

	key := (*C.uchar)(unsafe.Pointer(C.CString("__key__")))
	value := (*C.uchar)(unsafe.Pointer(C.CString("__value__")))

    C.lsm_write(db, key, value)

    resp := C.lsm_read(db, key)
    
	return_value := C.GoString((*C.char)(unsafe.Pointer(resp)))
    
    log.Printf("hello %x db w/ %s\n", db, return_value)

	s := <-sigint
	log.Printf("received %s signal\n", s)
	cancel()
	time.Sleep(500 * time.Millisecond)

}
