package kvnode

import (
	"log"
	"net/rpc"
	"time"
)

type workertable struct {
}

// struct to hold info about other kvnodes
type worker struct {
	IPPort     string
	Connection *rpc.Client
	isMaster   bool
	isAlive    bool
}

type TXInfo struct {
	ID  int
	Ops []TxCommand
}

// RPC calls for node->node communication
type NodeWorker interface {
	// Heartbeat to let you know a worker is alive
	Heartbeat(in int, out *bool) error
	// Inform a worker that a new TX has been created
	AddTX(in TXInfo, out *struct{}) error
	// Inform a Worker that a TX has been aborted
	AbortTX(in TXInfo, out *struct{}) error
	// Informa  worker that A TX has been commited
	CommitTX(in TXInfo, out *struct{}) error
}

// Inform a worker that a new TX has been created
func (N *KVNode) AddTX(in TXInfo, out *struct{}) error {
	nextIDLock.Lock()
	defer nextIDLock.Unlock()
	N.nextID = in.ID + 1
	N.addNewTX(in.ID)
	return nil
}

// Inform a Worker that a TX has been aborted
func (N *KVNode) AbortTX(in TXInfo, out *struct{}) error {
	t := N.transactionForID(in.ID)
	err := t.CheckTransaction()
	if err != nil {
		return err
	}
	t.Aborted = true
	return nil
}

// Informa  worker that A TX has been commited
func (N *KVNode) CommitTX(in TXInfo, out *struct{}) error {
	t := N.transactionForID(in.ID)
	err := t.CheckTransaction()
	if err != nil {
		return err
	}
	t.Commands = in.Ops
	t.Commited = true
	N.atomicallyDoPuts(t)
	return nil
}

func (w *worker) StartConnection() {
	var err error
	var c *rpc.Client
	for start := time.Now(); time.Since(start) < 4*time.Second; {
		c, err = rpc.Dial("tcp", w.IPPort)
		if err != nil {
			continue
		} else {
			w.Connection = c
			break
		}
	}
	if c == nil {
		log.Println("Unable to connect to ", w.IPPort, " err:", err)
		w.isAlive = false
	}
}

func (N *KVNode) workerForAddr(addr string) *worker {
	N.workerTableLock.RLock()
	defer N.workerTableLock.RUnlock()
	for _, w := range N.workers {
		if w.IPPort == addr {
			return w
		}
	}
	log.Fatal("Called WorkerForAddr with in invalid addr: ", addr)
	return nil
}

func (N *KVNode) broadcastAddTX(txid int) {
	N.workerTableLock.RLock()
	defer N.workerTableLock.RUnlock()
	if !N.isMaster {
		log.Fatal("Called Broadcast when not master")
	}
	for _, w := range N.workers {
		if w.isAlive {
			info := TXInfo{ID: txid}
			var out struct{}
			err := w.Connection.Call("NodeWorker.AddTX", info, &out)
			if err != nil {
				_ = w.Connection.Close()
				w.Connection = nil
				w.StartConnection()
				if w.isAlive {
					log.Println("Error contacting worker but it is reachable", err)
					err := w.Connection.Call("NodeWorker.AddTX", info, &out)
					if err != nil {
						log.Fatal("failed again", err)
					}
				}
			}
		}
	}
}

func (N *KVNode) broadcastAbortTX(txid int) {
	N.workerTableLock.RLock()
	defer N.workerTableLock.RUnlock()
	if !N.isMaster {
		log.Fatal("Called Broadcast when not master")
	}
	for _, w := range N.workers {
		if w.isAlive {
			info := TXInfo{ID: txid}
			var out struct{}
			err := w.Connection.Call("NodeWorker.AbortTX", info, out)
			if err != nil {
				_ = w.Connection.Close()
				w.Connection = nil
				w.StartConnection()
				if w.isAlive {
					log.Println("Error contacting worker but it is reachable", err)
					err := w.Connection.Call("NodeWorker.AbortTX", info, out)
					if err != nil {
						log.Fatal("failed again", err)
					}
				}
			}
		}
	}
}

func (N *KVNode) broadcastCommit(tx TXInfo) {
	N.workerTableLock.RLock()
	defer N.workerTableLock.RUnlock()
	if !N.isMaster {
		log.Fatal("Called Broadcast when not master")
	}
	for _, w := range N.workers {
		if w.isAlive {
			var out struct{}
			err := w.Connection.Call("NodeWorker.CommitTX", tx, &out)
			if err != nil {
				_ = w.Connection.Close()
				w.Connection = nil
				w.StartConnection()
				if w.isAlive {
					log.Println("Error contacting worker but it is reachable", err)
					err := w.Connection.Call("NodeWorker.CommitTX", tx, &out)
					if err != nil {
						log.Fatal("failed again", err)
					}
				}
			}
		}
	}
}
