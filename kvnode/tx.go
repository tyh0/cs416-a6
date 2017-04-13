package kvnode

import (
	"errors"
	"log"
	"sync"
	"time"
)

// Code dealing with transactions goes in here
var nextIDLock sync.Mutex

// Get the next transaction id
func (N *KVNode) getNextID() int {
	nextIDLock.Lock()
	defer nextIDLock.Unlock()
	temp := N.nextID
	N.nextID += 1
	return temp
}

func (N *KVNode) addNewTX(id int) {
	tx := new(Transaction)
	tx.TXID = id
	tx.LastHeartbeat = time.Now()
	N.transactionsLock.Lock()
	defer N.transactionsLock.Unlock()
	N.transactions[id] = tx
}

func (N *KVNode) transactionForID(id int) *Transaction {
	N.transactionsLock.RLock()
	defer N.transactionsLock.RUnlock()
	return N.transactions[id]
}

type Transaction struct {
	TXID          int
	Commands      []TxCommand
	Owner         string
	Aborted       bool
	Commited      bool
	LastHeartbeat time.Time
	Lock          sync.Mutex
}

const (
	GET = iota
	PUT
)

type TxCommand struct {
	Key   string // the key
	Value string // the value, if it is a put request
	Cmd   int    // GET or PUT
}

// lock the key for a given node
func (N *KVNode) getLock(t *Transaction, key string) error {
	return N.locks.LockKey(t.TXID, key)
	// now we have the lock
}

// Assuming that the lock is aquired for the transaction, return the value for the key
// first by searching through the commands, then by looking in the data store
func (N *KVNode) getValue(t *Transaction, key string) string {
	gotVal := false
	var res string
	// TODO would be more efficient to loop backwards
	for _, c := range t.Commands {
		if c.Cmd == PUT && c.Key == key {
			gotVal = true
			res = c.Value
		}
	}
	if !gotVal {
		N.datastoreLock.RLock()
		defer N.datastoreLock.RUnlock()
		res = N.datastore[key]
	}
	return res
}

func (t *Transaction) AppendCommand(cmd TxCommand) {
	// TODO should this be threadsafe?
	t.Commands = append(t.Commands, cmd)
}

var (
	AbortedError           error = errors.New("Transaction Aborted")
	CommitedError          error = errors.New("Transaction Already Commited")
	NoSuchTransactionError error = errors.New("No such Transaction")
)

// Returns nil if a Transaction should continue, or
// an appropriate error otherwise
func (t *Transaction) CheckTransaction() error {
	switch {
	case t == nil:
		return NoSuchTransactionError
	case t.Aborted:
		return AbortedError
	case t.Commited:
		return CommitedError
	default:
		return nil
	}
}

func (N *KVNode) atomicallyDoPuts(t *Transaction) int {
	N.datastoreLock.Lock()
	defer N.datastoreLock.Unlock()
	for _, cmd := range t.Commands {
		if cmd.Cmd == PUT {
			N.datastore[cmd.Key] = cmd.Value
		}
	}
	return t.TXID
}

// Called just before we become master
// set all previous transactions to aborted
func (N *KVNode) abortAllTransactions() {
	for _, t := range N.transactions {
		t.Aborted = true
	}
}

func (N *KVNode) updateHeartbeat(txid int) {
	t := N.transactionForID(txid)
	t.Lock.Lock()
	defer t.Lock.Unlock()
	t.LastHeartbeat = time.Now()
}

func (N *KVNode) startClientTimer() {
	ticker := time.NewTicker(time.Second)
	for _ = range ticker.C {
		if N.isMaster {
			N.checkTXTimes()
		}
	}
}

func (N *KVNode) checkTXTimes() {
	// in case there is a lot of contention, we want to base
	// our calculations on the time before we get the lock
	var toAbort []int
	nowtime := time.Now()
	N.transactionsLock.Lock()
	for _, t := range N.transactions {
		if !t.Commited && !t.Aborted {
			t.Lock.Lock()
			if nowtime.After(t.LastHeartbeat.Add(5 * time.Second)) {
				toAbort = append(toAbort, t.TXID)
			}
			t.Lock.Unlock()
		}
	}
	N.transactionsLock.Unlock()
	if len(toAbort) > 0 {
		log.Println(len(toAbort), " transactions timed out, aborting ", toAbort)
	}
	for _, id := range toAbort {
		var out bool
		_ = N.Abort(id, &out)
	}
}
