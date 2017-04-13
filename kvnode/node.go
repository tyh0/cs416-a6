package kvnode

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"sort"
	"sync"
	"time"

	"../common"
)

// A KVNode server. All global state should go into this struct
type KVNode struct {
	// the list of nodes from the nodesfile
	nodes []string
	// this node's id
	id int
	// this node's public address
	publicIPPort string

	// the ip:port combo this node listens for other nodes on
	nodeIPPort string
	// the ip:port combo this node listens for clients on
	clientIPPort string
	// the next id to assign
	nextID int
	// The key-value store and its lock
	datastore     map[string]string
	datastoreLock sync.RWMutex

	// The in-progress transaction state and its lock
	transactions     map[int]*Transaction
	transactionsLock sync.RWMutex

	// the lock state, only if we are txcoordinator
	locks LockMap

	// context for other nodes
	workers         []*worker
	workerTableLock sync.RWMutex
	currentMaster   *worker
	isMaster        bool
}

// create a new node
func NewNode(nodes []string, id int, nodeIPPort string, clientIPPort string) *KVNode {
	n := new(KVNode)
	var ourIP string
	othernodes := make([]string, 0, len(nodes)-1)
	for i, addr := range nodes {
		// these are 1 indexed
		nodeID := i + 1
		if nodeID == id {
			ourIP = addr
			othernodes = append(othernodes, addr)
		}
	}
	sort.StringSlice(nodes).Sort()

	n.nodes = nodes
	n.id = id
	n.publicIPPort = ourIP
	n.nodeIPPort = nodeIPPort
	n.clientIPPort = clientIPPort
	n.nextID = 1
	n.datastore = make(map[string]string)
	n.transactions = make(map[int]*Transaction)
	n.locks.locks = make(map[string]*Lock)
	return n
}

// Entry point for a KVNode. Start Serving
func Run(N *KVNode) {
	rpcServing := make(chan int)
	go N.startServingNodeRPC(rpcServing)
	<-rpcServing
	if !N.isMaster {
		go N.startMasterHeartbeat()
	}
	go N.startServingClientRPC()
	go N.startLockMapTimer()
	go N.startClientTimer()
	select {}

}

// Start serving node RPC and negotiate who master is
func (N *KVNode) startServingNodeRPC(done chan int) {
	N.workerTableLock.Lock()
	N.workerTableLock.Unlock()
	N.workers = make([]*worker, 0, len(N.nodes))
	for _, addr := range N.nodes {
		if addr == N.publicIPPort {
			// this is us
		} else {
			w := &worker{IPPort: addr, Connection: nil, isMaster: false, isAlive: true}
			w.StartConnection()
			N.workers = append(N.workers, w)
		}
	}
	// determine master
	for _, addr := range N.nodes {
		if addr == N.publicIPPort {
			N.isMaster = true
			break
		}
		w := N.workerForAddr(addr)
		if w.isAlive {
			w.isMaster = true
			N.currentMaster = w
			break
		}
	}
	rpcserver := rpc.NewServer()
	nwi := NodeWorker(N)

	rpcserver.RegisterName("NodeWorker", nwi)
	done <- 0
	log.Println("startingserving")
	for {
		conn, err := net.Listen("tcp", N.nodeIPPort)
		if err != nil {
			log.Fatal(err)
		}
		// This shouldn't return, but if for some reason it does we'll try again
		rpcserver.Accept(conn)
	}
}

func (N *KVNode) startMasterHeartbeat() {
	for {
		var in int
		var out bool
		err := N.currentMaster.Connection.Call("NodeWorker.Heartbeat", in, &out)
		if err != nil {
			_ = N.currentMaster.Connection.Close()
			N.currentMaster.Connection = nil
			N.currentMaster.StartConnection()
			if N.currentMaster.isAlive {
				log.Println("Heartbeat err from master: ", err, "but master is still reachable")
				continue
			}
			for _, addr := range N.nodes {
				if addr == N.publicIPPort {
					N.abortAllTransactions()
					N.isMaster = true
					log.Println("Becoming Master!")
					return
				}
				w := N.workerForAddr(addr)
				if w.isAlive {
					N.currentMaster = w
				}
			}
		}
		time.Sleep(time.Second / 2)
	}
}

func (N *KVNode) startServingClientRPC() {
	var ni common.NodeInterface
	// NOTE this makes sure that we are conforming to the common interface, it won't build if we don't
	ni = common.NodeInterface(N)
	rpcserver := rpc.NewServer()
	log.Println("Registering KVNode to rpcserver")
	err := rpcserver.RegisterName("KVNode", ni)
	if err != nil {
		log.Fatal("Register error: ", err)
	}
	for {
		log.Printf("Listening for clients on %v%n", N.clientIPPort)
		conn, err := net.Listen("tcp", N.clientIPPort)
		if err != nil {
			log.Fatal(err)
		}
		// This shouldn't return, but if for some reason it does we'll try again
		log.Println("Found a client connection")
		rpcserver.Accept(conn)
	}
}

func (N *KVNode) IsMaster(in struct{}, out *bool) error {
	*out = N.isMaster
	return nil
}

// RPC calls from client -> KVNode
func (N *KVNode) NewTransaction(in struct{}, out *int) error {
	ID := N.getNextID()
	N.addNewTX(ID)
	N.broadcastAddTX(ID)
	*out = ID
	return nil
}

// heartbeat to check if node is still alive
func (N *KVNode) Heartbeat(txid int, out *bool) error {
	if txid != 0 {
		N.updateHeartbeat(txid)
	}
	*out = true
	return nil
}

func (N *KVNode) Get(in common.CmdRequest, out *string) error {
	if !N.isMaster {
		return errors.New("Called get on node that is not master")
	}
	t := N.transactionForID(in.TXID)
	err := t.CheckTransaction()
	if err != nil {
		return err
	}
	if !N.locks.hasLock(t.TXID, in.Key) {
		err := N.getLock(t, in.Key)
		if err != nil {
			return err
		}
	}
	N.locks.Lock()
	defer N.locks.Unlock()
	if !N.locks.hasLockUnsafe(in.TXID, in.Key) {
		return AbortedError
	}
	*out = N.getValue(t, in.Key)
	var s string
	t.AppendCommand(TxCommand{Key: in.Key, Value: s, Cmd: GET})
	return nil
}

func (N *KVNode) Put(in common.CmdRequest, out *bool) error {
	log.Println("In Put for ", in.TXID)
	if !N.isMaster {
		log.Println("Called put on non-master node")
		return errors.New("Called put on node that is not master")
	}
	t := N.transactionForID(in.TXID)
	err := t.CheckTransaction()
	if err != nil {
		log.Println("Put error ", err, "for tx", in.TXID)
		return err
	}
	if !N.locks.hasLock(t.TXID, in.Key) {
		err := N.getLock(t, in.Key)
		if err != nil {
			// transaction was aborted to resolve a deadlock,
			// otherwise this would just block until we had the lock
			*out = false
			log.Println("Lock for ", t.TXID, " was aborted")
			return err
		}
	}
	N.locks.Lock()
	defer N.locks.Unlock()
	if !N.locks.hasLockUnsafe(in.TXID, in.Key) {
		*out = false
		log.Println("TX", in.TXID, "was aborted after aquiring")
		return AbortedError
	}
	t.AppendCommand(TxCommand{Key: in.Key, Value: in.Value, Cmd: PUT})
	*out = true
	return nil
}

func (N *KVNode) Abort(txid int, out *bool) error {
	if !N.isMaster {
		return errors.New("Called abort on node that is not master")
	}
	t := N.transactionForID(txid)
	err := t.CheckTransaction()
	if err != nil {
		return err
	}
	// we wnat to make sure to release all locks held by txid last using defer
	defer N.locks.Abort(txid)
	// then delete the key
	N.transactionsLock.Lock()
	t.Aborted = true
	N.transactionsLock.Unlock()
	N.broadcastAbortTX(txid)
	*out = true
	return nil
}

func (N *KVNode) Commit(txid int, out *int) error {
	if !N.isMaster {
		return errors.New("Called commit on node that is not master")
	}
	t := N.transactionForID(txid)
	err := t.CheckTransaction()
	if err != nil {
		return err
	}
	// we wnat to make sure to release all locks held by txid last using defer
	defer N.locks.Abort(txid)
	// but first modify data
	// sequenceNo is actually just TXID for now but that might change

	// This part is a critical section
	// to deal with race condition
	sequenceNo, err := func() (int, error) {
		N.locks.Lock()
		defer N.locks.Unlock()
		for _, op := range t.Commands {
			if !N.locks.hasLockUnsafe(txid, op.Key) {
				return 0, AbortedError
			}
		}
		sequenceNo := N.atomicallyDoPuts(t)
		info := TXInfo{ID: txid, Ops: t.Commands}
		N.broadcastCommit(info)
		return sequenceNo, nil
	}()
	if err != nil {
		t.Aborted = true
		return err
	}
	// then set the commited flag
	t.Commited = true
	*out = sequenceNo
	return nil
}
