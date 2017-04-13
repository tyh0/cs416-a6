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
	log.Println("STARTING TC2 TEST")
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	nodes := common.GetNodes(os.Args[1])
	c := kvservice.NewConnection(nodes)
	if c == nil {
		log.Fatal("Failed to create connection")
	}

	t, err := c.NewTX()
	success, err := t.Put("a", "a")
	if !success || err != nil {
		log.Println("Failure! Put failed ", err)
		os.Exit(-1)
	}
	success, _, err = t.Commit()
	if !success || err != nil {
		log.Println("Failure! Commit Failed ", err)
		os.Exit(-1)
	}

	t, err = c.NewTX()
	success, err = t.Put("b", "b")
	if !success || err != nil {
		log.Println("Failure! Put failed ", err)
		os.Exit(-1)
	}
	success, _, err = t.Commit()
	c.Close()
	log.Println("TC2 TEST SUCCESS ><><><><><><><><><>")
}
