package kvnode

import (
	"../common"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

type LockMap struct {
	locks   map[string]*Lock
	mapLock sync.Mutex // a mutex to allow multiple threads to access the LockMap
}

type Lock struct {
	holder  int          // transaction currently holding a lock
	waiters []lockWaiter // transactions waiting on a lock
}

type lockWaiter struct {
	id   int       // the txid of the waiter
	done chan bool // the channel to signal when the waiter aquires the lock
	// true signal means success getting lock
	// false signal means abort
}

func (lm *LockMap) NumLocks() int {
	lm.mapLock.Lock()
	defer lm.mapLock.Unlock()
	return len(lm.locks)
}

func (lm *LockMap) Lock() {
	lm.mapLock.Lock()
}

func (lm *LockMap) Unlock() {
	lm.mapLock.Unlock()
}

// block until a lock is available
func (lm *LockMap) LockKey(txid int, key string) error {
	done := make(chan bool)
	go func() {
		lm.mapLock.Lock()
		defer lm.mapLock.Unlock()
		lock := lm.locks[key]
		if lock == nil {
			lock = new(Lock)
			lock.holder = txid
			lm.locks[key] = lock
			done <- true
			return
		}
		// if we're here, somebody holds the lock
		if lock.holder == txid {
			done <- true
		} else {
			alreadyWaiting := false
			for i, _ := range lock.waiters {
				if lock.waiters[i].id == txid {
					alreadyWaiting = true
				}
			}
			if !alreadyWaiting {
				lock.waiters = append(lock.waiters, lockWaiter{id: txid, done: done})
			} else {
				log.Println("ERROR: Called lock for ", txid, "on ", key, "when it was already waiting")
			}
		}
	}()
	gotLock := <-done
	if gotLock {
		log.Println("Got lock for tx:", txid, "key:", key)
		return nil
	}
	log.Println("Did not get lock for tx:", txid, "key:", key)
	return errors.New("Aborted")
}

func (lm *LockMap) hasLock(txid int, key string) bool {
	lm.mapLock.Lock()
	defer lm.mapLock.Unlock()
	return lm.hasLockUnsafe(txid, key)
}

// Must be called with lock held
func (lm *LockMap) hasLockUnsafe(txid int, key string) bool {
	l := lm.locks[key]
	if l == nil {
		return false
	}
	if l.holder == txid {
		return true
	}
	return false
}

// release a lock if you hold it
// remove tx from waitlist if you don't
func (lm *LockMap) Release(txid int, key string) {
	lm.mapLock.Lock()
	defer lm.mapLock.Unlock()
	doRelease(lm.locks, txid, key)
}

// do the release (this needs to be called by a function that has unlocked
// the mutex
func doRelease(locks map[string]*Lock, txid int, key string) {
	t := locks[key]
	if t.holder != txid {
		log.Println("Unexpected txid holding lock for ", key)
		return
	}
	if len(t.waiters) == 0 {
		delete(locks, key)
		return
	}
	t.holder = t.waiters[0].id
	donechan := t.waiters[0].done
	t.waiters = t.waiters[1:]
	go func(c chan bool) {
		c <- true
	}(donechan)
}

// find and release all locks held by txid
// and remove it from all waitlists
func (lm *LockMap) Abort(txid int) {
	log.Println("aborting ", txid)
	lm.mapLock.Lock()
	defer lm.mapLock.Unlock()
	var toRelease []string
	var waitingOn []*Lock
	for key, lock := range lm.locks {
		if lock.holder == txid {
			toRelease = append(toRelease, key)
		}
		for _, waitblock := range lock.waiters {
			if waitblock.id == txid {
				waitingOn = append(waitingOn, lock)
			}
		}
	}

	for _, lock := range waitingOn {
		doRemoveWaiter(lock, txid)
	}

	for _, k := range toRelease {
		doRelease(lm.locks, txid, k)
	}
}

// remove a waiter from a lock
// assumes mutex is already held
func doRemoveWaiter(lock *Lock, txid int) {
	idx := -1
	for i, waiter := range lock.waiters {
		if waiter.id == txid {
			idx = i
			break
		}
	}
	if idx == -1 {
		log.Println("Expected to find waiter but didn't")
		return
	}
	// abort
	go func(c chan bool) {
		c <- false
	}(lock.waiters[idx].done)
	lock.waiters = append(lock.waiters[:idx], lock.waiters[idx+1:]...)
}

// constants for node type
const (
	TX = iota
	KEY
)

type graphNode struct {
	nodetype      int
	txid          int    // 0 if nodetype is key
	key           string // empty if nodetype is tx
	incomingEdges int    // place to store count of incoming edges
	rank          int    // if it is a txnode, the number of locks it is a holder of
}

type graphEdge struct {
	from *graphNode
	to   *graphNode
}

type nodeGraph struct {
	txnodes  map[int]*graphNode
	keynodes map[string]*graphNode
	edges    []*graphEdge
}

// this doesn't return
// run a timer and periodically check for deadlocks and issue aborts
func (N *KVNode) startLockMapTimer() {
	// might have to finetune this value a bit
	ticker := time.NewTicker(time.Second / 10)
	for {
		<-ticker.C
		if N.locks.NumLocks() > 1 {
			N.locks.HandleDeadlocks(N)
		}
	}
}

// traverse the wait graph
// clear out any waiters that should be removed and signal
// false to their done channels
func (lm *LockMap) HandleDeadlocks(N *KVNode) {
	lm.mapLock.Lock()
	graph := newnodeGraph()
	// first, construct the graph
	for key, lock := range lm.locks {
		if lock != nil { // just to be safe
			graph.addKeyNode(key)
			graph.addTXNode(lock.holder)
			graph.addHolderEdge(lock.holder, key)
			for _, waiter := range lock.waiters {
				graph.addTXNode(waiter.id)
				graph.addWaiterEdge(waiter.id, key)
			}
		}
	}
	// NOTE this is a very ugly, slow but understandable algorithm
	// it will actually be relatively fast for a disconected wait graph, but very slow in the worst case if there are a lot of conflicts
	var toAbort []int
	graph.countIncomingEdges()
	for count := graph.countNodes(); count > 0; count = graph.countNodes() {
		for {
			before := graph.countNodes()
			// next we want to count how many edges are incoming to all nodes
			// any node that doesn't have any incoming edges is safe and can be discarded
			// (this means if it is a tx it isn't holding any locks, and if it's a key it's not being waited on)
			graph.pruneNodes()
			after := graph.countNodes()
			if after == 0 || after == before {
				break
			}
		}
		if graph.countNodes() > 0 {
			// at this point all nodes in the graph are part of one cycle or another
			// so we should abort the lowest ranked transaction and remove it from the graph
			id := graph.removeLowestRankedTransaction()
			toAbort = append(toAbort, id)
		}
	}
	lm.mapLock.Unlock()
	if len(toAbort) > 0 {
		log.Println("Aborting ", len(toAbort), " transactions", toAbort)
	}
	for _, id := range toAbort {
		var out bool
		_ = N.Abort(id, &out)
	}
}

func newnodeGraph() *nodeGraph {
	return &nodeGraph{txnodes: make(map[int]*graphNode),
		keynodes: make(map[string]*graphNode),
	}
}

// add node for key if it doesn't already exist
func (g *nodeGraph) addKeyNode(key string) {
	n := g.keynodes[key]
	if n == nil {
		n = new(graphNode)
		n.key = key
		n.nodetype = KEY
		g.keynodes[key] = n
	}
}

func (g *nodeGraph) addTXNode(txid int) {
	n := g.txnodes[txid]
	if n == nil {
		n = new(graphNode)
		n.txid = txid
		n.nodetype = TX
		g.txnodes[txid] = n
	}
}

func (g *nodeGraph) addHolderEdge(txid int, key string) {
	g.txnodes[txid].rank += 1
	edge := new(graphEdge)
	edge.from = g.keynodes[key]
	edge.to = g.txnodes[txid]
	g.edges = append(g.edges, edge)
}

func (g *nodeGraph) addWaiterEdge(txid int, key string) {
	edge := new(graphEdge)
	edge.from = g.txnodes[txid]
	edge.to = g.keynodes[key]
	g.edges = append(g.edges, edge)
}

// count incoming edges for each node
func (g *nodeGraph) countIncomingEdges() {
	for i, _ := range g.edges {
		g.edges[i].to.incomingEdges += 1
	}
}

// remove any nodes with 0 incoming edges
func (g *nodeGraph) pruneNodes() {
	var toDelete []*graphNode
	for _, node := range g.keynodes {
		if node.incomingEdges == 0 {
			toDelete = append(toDelete, node)
		}
	}
	for _, node := range g.keynodes {
		if node.incomingEdges == 0 {
			toDelete = append(toDelete, node)
		}
	}
	for len(toDelete) > 0 {
		for _, n := range toDelete {
			switch n.nodetype {
			case TX:
				delete(g.txnodes, n.txid)
			case KEY:
				delete(g.keynodes, n.key)
			}
		}
		toDelete = nil
		// Now we prune the edges
		newEdges := make([]*graphEdge, 0, (len(g.edges)*3)/5)
		for _, e := range g.edges {
			shouldDelete := false
			switch e.from.nodetype {
			case TX:
				if g.txnodes[e.from.txid] == nil {
					// delete the edge
					shouldDelete = true
				}
			case KEY:
				if g.keynodes[e.from.key] == nil {
					// delete the edge
					shouldDelete = true
				}
			}
			if shouldDelete {
				e.to.incomingEdges -= 1
				if e.to.incomingEdges == 0 {
					toDelete = append(toDelete, e.to)
				}
			} else {
				newEdges = append(newEdges, e)
			}
		}
		g.edges = newEdges
	}
}

func (g *nodeGraph) countNodes() int {
	return len(g.keynodes) + len(g.txnodes)
}

// remove the lowest ranked transaction from the graph and any edges connected
// to it, return its id
func (g *nodeGraph) removeLowestRankedTransaction() int {
	max := common.MaxInt
	var lowest *graphNode
	var lowID int
	for id, n := range g.txnodes {
		if n.rank < max {
			max = n.rank
			lowest = n
			lowID = id
		}
	}
	if lowest == nil {
		log.Fatal("called removeLowestRankedTransaction with no nodes")
	}
	delete(g.txnodes, lowID)
	newEdges := make([]*graphEdge, 0, len(g.edges))
	for _, e := range g.edges {
		if e.to != lowest && e.from != lowest {
			newEdges = append(newEdges, e)
		} else {
			// deleting e, so update it's destinations incoming edges number
			e.to.incomingEdges -= 1
		}
	}
	g.edges = newEdges
	return lowID
}

func (g *nodeGraph) String() string {
	res := "{"
	for i, e := range g.edges {
		if i != 0 {
			res += ", "
		}
		switch e.from.nodetype {
		case TX:
			res += fmt.Sprint(e.from.txid, "is waiting on ", e.to.key)
		case KEY:
			res += fmt.Sprint(e.to.txid, " holds ", e.from.key)
		}
	}
	res += "}"
	res += "\n"
	res += func() string {
		r := ""
		for _, node := range g.txnodes {
			r += fmt.Sprint("TXID: ", node.txid, "IncomingEdges: ", node.incomingEdges)
		}
		return r
	}()
	res += "\n"
	res += func() string {
		r := ""
		for _, node := range g.keynodes {
			r += fmt.Sprint("Key: ", node.key, "IncomingEdges: ", node.incomingEdges)
		}
		return r
	}()
	return res
}
