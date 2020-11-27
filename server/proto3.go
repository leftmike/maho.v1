package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"

	pgproto3 "github.com/jackc/pgproto3/v2"
	"github.com/lib/pq/oid"
	log "github.com/sirupsen/logrus"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

type Proto3Config struct {
	Address string
}

func (svr *Server) ListenAndServeProto3(p3Cfg Proto3Config) error {
	l, err := net.Listen("tcp", p3Cfg.Address)
	if err != nil {
		return err
	}
	svr.addListener(l)

	for {
		conn, err := l.Accept()
		if err != nil {
			svr.mutex.Lock()
			if svr.shutdown {
				err = ErrServerClosed
			}
			svr.mutex.Unlock()
			log.WithField("error", err.Error()).Error("proto3 accept")
			return err
		}

		entry := log.WithFields(log.Fields{
			"addr": conn.RemoteAddr().String(),
		})
		entry.Info("proto3 connected")

		go svr.handleProto3Conn(conn, entry)
	}

	return nil
}

func (svr *Server) handleProto3Conn(conn net.Conn, entry *log.Entry) {
	atomic.AddInt32(&svr.connCount, 1)
	defer atomic.AddInt32(&svr.connCount, -1)

	defer func() {
		entry.Info("proto3 disconnected")
	}()

	if !svr.trackConn(conn, true) {
		conn.Close()
		return
	}

	defer func() {
		if svr.trackConn(conn, false) {
			conn.Close()
		}
	}()

	be := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	var started bool
	for !started {
		msg, err := be.ReceiveStartupMessage()
		if err != nil {
			entry.Errorf("receive startup message: %s", err)
			break
		}

		switch msg := msg.(type) {
		case *pgproto3.StartupMessage:
			entry.Infof("protocol version: %d", msg.ProtocolVersion)
			for nam, val := range msg.Parameters {
				entry.Infof("parameter: %s = %s", nam, val)
			}
			_, err := conn.Write((&pgproto3.AuthenticationOk{}).Encode(nil))
			if err != nil {
				entry.Errorf("send authentication ok: %s", err)
				break
			}
			started = true
		case *pgproto3.SSLRequest:
			_, err := conn.Write([]byte("N"))
			if err != nil {
				entry.Errorf("send deny SSL request: %s", err)
				break
			}
		default:
			entry.Errorf("unknown startup message: %v", msg)
			break
		}
	}

	if started {
		svr.HandleSession(func(ses *evaluate.Session) {
			handleProto3Session(ses, be, conn, entry)
		}, "user", "proto3", "addr") // XXX
	}
}

func dataType(ct sql.ColumnType) (oid.Oid, int16, int32) {
	// Return oid, size, and type modifier.
	switch ct.Type {
	case sql.UnknownType:
		return oid.T_text, -1, -1
	case sql.BooleanType:
		return oid.T_bool, 1, -1
	case sql.StringType:
		if ct.Fixed {
			return oid.T_bpchar, -1, int32(ct.Size) + 4
		} else if ct.Size < sql.MaxColumnSize {
			return oid.T_varchar, -1, int32(ct.Size) + 4
		} else {
			return oid.T_text, -1, -1
		}
	case sql.BytesType:
		return oid.T_bytea, -1, -1
	case sql.FloatType:
		if ct.Size == 4 {
			return oid.T_float4, 4, -1
		} else {
			return oid.T_float8, 8, -1
		}
	case sql.IntegerType:
		switch ct.Size {
		case 2:
			return oid.T_int2, 2, -1
		case 4:
			return oid.T_int4, 4, -1
		default:
			return oid.T_int8, 8, -1
		}
	default:
		panic(fmt.Sprintf("unexpected column type; got %#v", ct))
	}
}

func handleProto3Session(ses *evaluate.Session, be *pgproto3.Backend, conn net.Conn,
	entry *log.Entry) {

	for {
		var ch byte
		if ses.ActiveTx() {
			ch = 'T'
		} else {
			ch = 'I'
		}
		_, err := conn.Write((&pgproto3.ReadyForQuery{TxStatus: ch}).Encode(nil))
		if err != nil {
			entry.Errorf("send ready for query: %s", err)
			break
		}

		msg, err := be.Receive()
		if err != nil {
			if err != io.EOF {
				entry.Errorf("receive: %s", err)
			}
			break
		}

		switch msg := msg.(type) {
		case *pgproto3.Query:
			proto3Query(ses, conn, msg, entry)
		case *pgproto3.Terminate:
			break
		default:
			buf, _ := json.Marshal(msg)
			entry.Errorf("backend unexpected message: %s", string(buf))
		}
	}
}

func proto3Query(ses *evaluate.Session, conn net.Conn, msg *pgproto3.Query, entry *log.Entry) {
	p := parser.NewParser(strings.NewReader(msg.String), "proto3")
	stmt, err := p.Parse()
	if (stmt == nil && err == nil) || err == io.EOF {
		_, err := conn.Write((&pgproto3.EmptyQueryResponse{}).Encode(nil))
		if err != nil {
			entry.Errorf("send empty query response: %s", err)
		}
		return
	} else if err != nil {
		proto3ErrorResponse(conn, err, entry)
		return
	}

	err = ses.Run(stmt,
		func(ctx context.Context, ses *evaluate.Session, e sql.Engine,
			tx sql.Transaction) error {
			plan, err := stmt.Plan(ctx, ses, tx)
			if err != nil {
				return err
			}
			if stmtPlan, ok := plan.(evaluate.StmtPlan); ok {
				n, err := stmtPlan.Execute(ctx, tx)
				if err != nil {
					return err
				}
				proto3CommandComplete(conn, stmtPlan.Tag(), n, entry)
			} else if cmdPlan, ok := plan.(evaluate.CmdPlan); ok {
				err = cmdPlan.Command(ctx, ses, e)
				if err != nil {
					return err
				}
				proto3CommandComplete(conn, cmdPlan.Tag(), -1, entry)
			} else if rowsPlan, ok := plan.(evaluate.RowsPlan); ok {
				var fields []pgproto3.FieldDescription
				cols := rowsPlan.Columns()
				colTypes := rowsPlan.ColumnTypes()
				for cdx := range cols {
					oid, sz, tmod := dataType(colTypes[cdx])
					fields = append(fields,
						pgproto3.FieldDescription{
							Name:                 []byte(cols[cdx].String()),
							TableOID:             0,
							TableAttributeNumber: 0,
							DataTypeOID:          uint32(oid),
							DataTypeSize:         sz,
							TypeModifier:         tmod,
							Format:               0, // Text format; binary format = 1
						})
				}
				_, err := conn.Write((&pgproto3.RowDescription{Fields: fields}).Encode(nil))
				if err != nil {
					entry.Errorf("send row description: %s", err)
					return err
				}

				rows, err := rowsPlan.Rows(ctx, tx)
				if err != nil {
					return err
				}

				values := make([][]byte, len(cols))
				dest := make([]sql.Value, len(cols))
				var cnt int64
				for {
					err = rows.Next(ctx, dest)
					if err != nil {
						break
					}

					for vdx, v := range dest {
						if v == nil {
							values[vdx] = nil
						} else {
							switch v := v.(type) {
							case sql.StringValue:
								values[vdx] = []byte(string(v))
							case sql.BytesValue:
								values[vdx] = v.HexBytes()
							default:
								values[vdx] = []byte(sql.Format(v))
							}
						}
					}

					_, err := conn.Write((&pgproto3.DataRow{Values: values}).Encode(nil))
					if err != nil {
						entry.Errorf("send data row: %s", err)
						return err
					}

					cnt += 1
				}

				if err != io.EOF {
					return err
				}

				proto3CommandComplete(conn, rowsPlan.Tag(), cnt, entry)
			} else {
				panic(fmt.Sprintf("expected StmtPlan, CmdPlan, or RowsPlan: %#v", plan))
			}

			return nil
		})

	if err != nil {
		proto3ErrorResponse(conn, err, entry)
	}
}

func proto3ErrorResponse(conn net.Conn, err error, entry *log.Entry) {
	_, cerr := conn.Write((&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Message:  err.Error(),
	}).Encode(nil))
	if cerr != nil {
		entry.Errorf("send error response: %s", cerr)
	}
}

func proto3CommandComplete(conn net.Conn, tag string, n int64, entry *log.Entry) {
	var cmdTag string
	if n < 0 {
		cmdTag = tag
	} else if tag == "INSERT" {
		cmdTag = fmt.Sprintf("%s 0 %d", tag, n)
	} else {
		cmdTag = fmt.Sprintf("%s %d", tag, n)
	}
	_, err := conn.Write((&pgproto3.CommandComplete{CommandTag: []byte(cmdTag)}).Encode(nil))
	if err != nil {
		entry.Errorf("send command complete: %s", err)
	}
}
