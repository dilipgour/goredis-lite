package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {
	ln, err := net.Listen("tcp", ":3000")
	if err != nil {
		panic(err)
	}

	fmt.Println("Server running on port 3000")

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	resp := NewResp(conn)
	writer := NewWriter(conn)
	aof, err := NewAof("./database.aof")

	defer aof.Close()

	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		// Read a single RESP request
		value, err := resp.Read()
		if err != nil {
			fmt.Println("Client closed:", err)
			return
		}

		// Must be an array: ["COMMAND", arg1, arg2...]
		if value.typ != "array" || len(value.arr) == 0 {
			writer.Write(Value{typ: "error", str: "ERR invalid command"})
			continue
		}

		// Extract command
		command := strings.ToUpper(string(value.arr[0].bulk))
		args := value.arr[1:]

		handler, ok := Handlers[command]
		if !ok {
			writer.Write(Value{typ: "error", str: "ERR unknown command"})
			continue
		}

		if command == "SET" || command == "HSET" {
			aof.Write(value)
		}

		// Write proper RESP response
		if err := writer.Write(handler(args)); err != nil {
			fmt.Println("Write error:", err)
			return
		}
	}
}
