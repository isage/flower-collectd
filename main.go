package main

import (
  "flag"
  "log"
  "net"
  "os"
  "os/signal"
  "runtime"
  "sync"
  "sync/atomic"
  "time"
  "encoding/json"
  "fmt"
  "github.com/calmh/ipfix"
  "github.com/roistat/go-clickhouse"
)

const (
  flushInterval = time.Duration(1) * time.Second
  maxQueueSize  = 1000000
  UDPPacketSize = 65507
)

var address string
var bufferPool sync.Pool
var ops uint64 = 0
var total uint64 = 0
var flushTicker *time.Ticker
var nbWorkers int
var schema Schema
var chConn *clickhouse.Conn

func init() {

  flag.StringVar(&address, "addr", ":2055", "UDP port to listen")
  flag.IntVar(&nbWorkers, "concurrency", runtime.NumCPU(), "Number of workers to run in parallel")
  file, err := os.Open("schema.json");
  if err != nil {
    panic(err)
  }

  dec := json.NewDecoder(file)
  if err := dec.Decode(&schema); err != nil {
    panic(err)
  }

  if debug {
    dl.Println(schema)
  }

  chConn = clickhouse.NewConn("localhost:8123", clickhouse.NewHttpTransport())
  err = chConn.Ping()
  if err != nil {
    panic(err)
  }

  for _, tableData := range schema.Tables {
    query := clickhouse.NewQuery(tableData.Marshal())
    err := query.Exec(chConn)
    if err != nil {
        panic(err)
    }
  }

}

type message struct {
  addr   net.Addr
  msg    []byte
  length int
}

type messageQueue chan message

func (mq messageQueue) enqueue(m message) {
  mq <- m
}

func (mq messageQueue) dequeue( inter *ipfix.Interpreter, sess *ipfix.Session) {
  for m := range mq {
    handleMessage(m.addr, m.msg[:m.length], inter, sess)
    bufferPool.Put(m.msg)
  }
}

var mq messageQueue

func main() {

//  runtime.GOMAXPROCS(runtime.NumCPU())
  flag.Parse()

  sess := ipfix.NewSession()
  inter := ipfix.NewInterpreter(sess)


  bufferPool = sync.Pool{
    New: func() interface{} { return make([]byte, UDPPacketSize) },
  }
  mq = make(messageQueue, maxQueueSize)
  listenAndReceive(nbWorkers, inter, sess)

  c := make(chan os.Signal, 1)
  signal.Notify(c, os.Interrupt)
  go func() {
    for range c {
      atomic.AddUint64(&total, ops)
      log.Printf("Total ops %d", total)
      os.Exit(0)
    }
  }()

  flushTicker = time.NewTicker(flushInterval)
  for range flushTicker.C {
    atomic.AddUint64(&total, ops)
    atomic.StoreUint64(&ops, 0)
  }
}

func listenAndReceive(maxWorkers int, inter *ipfix.Interpreter, sess *ipfix.Session) error {

  c, err := net.ListenPacket("udp", address)
  if err != nil {
    return err
  }
  for i := 0; i < maxWorkers; i++ {
    go mq.dequeue(inter, sess)
    go receive(c)
  }
  return nil
}

// receive accepts incoming datagrams on c and calls handleMessage() for each message
func receive(c net.PacketConn) {

  defer c.Close()

  for {
    msg := bufferPool.Get().([]byte)
    nbytes, addr, err := c.ReadFrom(msg[0:])
    if err != nil {
      log.Printf("Error %s", err)
      continue
    }
    mq.enqueue(message{addr, msg, nbytes})
  }
}

func handleMessage(addr net.Addr, msg []byte, inter *ipfix.Interpreter, sess *ipfix.Session) {

  mesg, err := sess.ParseBuffer(msg)
  if err != nil {
    panic(err)
  }

  var fieldList []ipfix.InterpretedField

  if len(mesg.DataRecords) > 0 {
    for _, table := range schema.Tables {

      columns := table.GetColumns()
      var rows []clickhouse.Row

      for _, record := range mesg.DataRecords {
        fieldList = inter.InterpretInto(record, fieldList)
        rows = append(rows, table.PrepareRow(fieldList))
      }

      query, err := clickhouse.BuildMultiInsert(fmt.Sprintf("%s.%s", table.Db, table.Name),
        columns,
        rows,
      )

      if err != nil {
        panic(err)
      }

      err = query.Exec(chConn)
      if err != nil {
          panic(err)
      }
    }
  }

  atomic.AddUint64(&ops, 1)
}
