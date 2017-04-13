/*
	Usage: go run kvnode.go [nodesFile] [nodeID] [listen-node-in IP:port] [listen-client-in IP:port]
*/
package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"./common"
	"./kvnode"
)

func usageAndQuit() {
	fmt.Println("go run kvnode.go [nodesFile] [nodeID] [listen-node-in IP:port] [listen-client-in IP:port]")
	os.Exit(-1)
}

func main() {
	// make logger print out line numbers
	// log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetFlags(log.Lshortfile)

	args := os.Args[1:]
	if len(args) != 4 {
		usageAndQuit()
	}
	nodeFileName := args[0]
	nodes := common.GetNodes(nodeFileName)
	nodeID, err := strconv.Atoi(args[1])
	log.SetPrefix("[KVNode " + fmt.Sprint(args[1]) + "] ")
	if err != nil {
		log.Fatal(err)
	}
	listenNodeIn := args[2]
	listenClientIn := args[3]
	log.Printf("Creating node using kvnode.NewNode on %v, %v, %v, %v\n", nodes, nodeID, listenNodeIn, listenClientIn)
	node := kvnode.NewNode(nodes, nodeID, listenNodeIn, listenClientIn)
	kvnode.Run(node) // shouldn't return
}
