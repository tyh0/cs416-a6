package common

// An RPC Request from client to server
type CmdRequest struct {
	TXID  int
	Key   string
	Value string
}

// this lists the RPC methods that a KVNode has which are accessible from the client
type NodeInterface interface {
	NewTransaction(in struct{}, out *int) error
	Get(in CmdRequest, out *string) error
	Put(in CmdRequest, out *bool) error
	Abort(txid int, out *bool) error
	Commit(txid int, out *int) error
	Heartbeat(txid int, out *bool) error
	IsMaster(in struct{}, out *bool) error
}
