package common

import (
	"io/ioutil"
	"log"
	"strings"
)

// read the node file and return a slice containing the lines of the nodefile
func GetNodes(fname string) []string {
	b, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Fatal(err)
	}
	split := strings.Split(string(b), "\n")
	res := make([]string, 0, len(split))
	for _, s := range split {
		if s != "" && s != "\n" {
			res = append(res, s)
		}
	}
	return res
}
