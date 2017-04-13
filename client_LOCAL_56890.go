/*

A trivial client to illustrate how the kvservice library can be used
from an application in assignment 6 for UBC CS 416 2016 W2.

Usage:
go run client.go
*/

package main

// Expects kvservice.go to be in the ./kvservice/ dir, relative to
// this client.go file
import (
	"./common"
	"./kvservice"
	"fmt"
	"log"
	"os"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	nodes := common.GetNodes(os.Args[1])
	c := kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)

	t, err := c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	success, err := t.Put("hello", "world")
	fmt.Printf("Put returned: %v, %v\n", success, err)

	success, v, err := t.Get("hello")
	fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

	success, txID, err := t.Commit()
	fmt.Printf("Commit returned: %v, %v, %v\n", success, txID, err)

	deadlocktest(c)

	c.Close()
}

func deadlocktest(c kvservice.Connection) {
	log.Println("Starting deadlock test")
	failedTest := func(explanation string, err error) {
		if err != nil {
			log.Println("Deadlock test failed: ", explanation, " ", err)
			return
		}
		log.Println("Deadlock test failed: ", explanation)
	}
	t, err := c.NewTX()
	if err != nil {
		failedTest("Error creating tx", err)
		return
	}
	success, err := t.Put("a", "a")
	if err != nil {
		failedTest("Put", err)
		return
	}
	success, err = t.Put("b", "b")
	if err != nil {
		failedTest("Put", err)
		return
	}
	success, _, err = t.Commit()
	if !success {
		failedTest("commit failed ", err)
		return
	}
	tx1, err := c.NewTX()
	if err != nil {
		failedTest("Error creating tx", err)
		return
	}
	tx2, err := c.NewTX()
	if err != nil {
		failedTest("Error creating tx", err)
		return
	}
	_, a, err := tx1.Get("a")
	if a != "a" {
		failedTest("Get should return a", nil)
		return
	}
	_, b, err := tx2.Get("b")
	if b != "b" {
		failedTest("Get should return b", nil)
		return
	}
	ch := make(chan bool, 2)
	var s1, s2 bool
	var e1, e2 error
	go func(ch chan bool) {
		s1, e1 = tx1.Put("b", "notb")
		ch <- true
	}(ch)
	go func(ch chan bool) {
		s2, e2 = tx2.Put("a", "nota")
		ch <- true
	}(ch)
	<-ch
	<-ch
	log.Println("Finished deadlock")
	if s1 && s2 {
		failedTest("only one should have succeeded", nil)
	}
	if !s1 && !s2 {
		failedTest("at least one should have succeeded", nil)
	}
	if e1 != nil || e2 != nil {
		log.Println("e1: ", e1, "e2: ", e2)
	}
	if (s1 && !s2) || (!s1 && s2) {
		log.Println("Test Passed, one transaction was broken")
	}
	tx1.Abort()
	tx2.Abort()
}
