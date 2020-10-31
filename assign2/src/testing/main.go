package main

import "fmt"

type PaxosNum struct {
	Id int
	Num int
}

func main() {
	c := make(chan *PaxosNum)
	for i := 0; i < 5; i++ {
		c <- &PaxosNum{0, 0}
		fmt.Printf("%d\n", i)
	}
}
