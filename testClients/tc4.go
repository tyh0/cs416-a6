/*
	This client adds a value "a":"a" then exits
*/
package main

// Expects kvservice.go to be in the ./kvservice/ dir, relative to
// this client.go file
import (
	"log"
	"os"

	"../common"
	"../kvservice"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	nodes := common.GetNodes(os.Args[1])
	c := kvservice.NewConnection(nodes)
	defer c.Close()
	if c == nil {
		log.Fatal("Failed to create connection")
	}

	t, err := c.NewTX()
	_, val, err := t.Get("a")
	if err != nil {
		log.Fatal("Failure, get err", err)
	}
	if val == "b" {
		log.Fatal("Failure! 'b' got commited")
	}
	log.Println("TC4 - I should get here too!")
}
