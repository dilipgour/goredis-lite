package main

import (
	"fmt"
	"strconv"
	"sync"
)

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"LPUSH":   lpush,
	"LPOP":    lpop,
	"RPUSH":   rpush,
	"RPOP":    rpop,
	"LRANGE":  lrange,
	"LINDEX":  lindex,
	"COMMAND": command,
}

func command(args []Value) Value {

	return Value{typ: "bulk", bulk: "OK"}
}

func ping(args []Value) Value {
	return Value{
		typ: "string",
		str: "PONG",
	}
}

// func ping(args []Value) Value {

// 	if len(args) == 0 {
// 		return Value{typ: "string", str: "PONG"}
// 	}
// 	return Value{typ: "string", str: []byte(args[0].bulk)}
// }

var SETs = map[string]string{}
var SETSmu sync.RWMutex

func set(args []Value) Value {

	if len(args) != 2 {
		return Value{typ: "error", str: "ERROR : wrong number of argumments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	SETSmu.Lock()
	SETs[key] = value
	SETSmu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERROR : wrong number of argumments for 'get' command"}

	}
	key := args[0].bulk

	SETSmu.RLock()

	value, ok := SETs[key]
	SETSmu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

var HSETs = map[string]map[string]string{}
var HSETSmu sync.RWMutex

func hset(args []Value) Value {
	if len(args) < 3 {
		return Value{typ: "error", str: "ERROR : wrong number of argumments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETSmu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETSmu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERROR : wrong number of argumments for 'hget' command"}

	}

	hashKey := args[0].bulk
	fieldKey := args[1].bulk

	HSETSmu.RLock()

	fields, ok := HSETs[hashKey]
	if !ok {
		HSETSmu.RUnlock()
		return Value{typ: "null"}
	}

	value, ok := fields[fieldKey]
	HSETSmu.RUnlock()

	if !ok {
		fmt.Println("inside null conditiion")
		fmt.Println("inside null conditiion")

		return Value{typ: "null"}
	}

	fmt.Println("outside the null:  ", value)

	return Value{typ: "bulk", bulk: value}

}

type Node struct {
	values []string
	next   *Node
	prev   *Node
}
type QuickList struct {
	head    *Node
	tail    *Node
	maxNode int
	length  int
}

var LISTs = map[string]*QuickList{}
var Listsmu sync.RWMutex

func newQuickList(maxsize int) *QuickList {
	return &QuickList{maxNode: maxsize}

}

func lpush(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERROR : wrong number of argumments for 'lpush' command"}
	}

	key := args[0].bulk
	Listsmu.Lock()
	defer Listsmu.Unlock()
	_, ok := LISTs[key]

	if !ok {
		LISTs[key] = newQuickList(8)
	}

	for i := 1; i < len(args); i++ {
		LISTs[key].LPush(args[i].bulk)

	}

	msg := fmt.Sprintf("Integer(%d)", LISTs[key].length)

	return Value{typ: "string", str: msg}

}

func (ql *QuickList) LPush(value string) {
	if ql.head == nil {
		ql.head = &Node{
			values: []string{value},
			next:   nil,
			prev:   nil,
		}
		ql.tail = ql.head
	} else if len(ql.head.values) < ql.maxNode {
		ql.head.values = append([]string{value}, ql.head.values...)
	} else {
		newNode := &Node{
			values: []string{value},
			next:   ql.head,
			prev:   nil,
		}
		ql.head.prev = newNode
		ql.head = newNode
	}
	ql.length++
}

func rpush(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERROR : wrong number of argumments for 'lpush' command"}
	}

	key := args[0].bulk
	Listsmu.Lock()
	defer Listsmu.Unlock()
	_, ok := LISTs[key]

	if !ok {
		LISTs[key] = newQuickList(8)
	}

	for i := 1; i < len(args); i++ {
		LISTs[key].LPush(args[i].bulk)

	}

	msg := fmt.Sprintf("Integer(%d)", 5)

	return Value{typ: "string", str: msg}

}

func (ql *QuickList) RPush(value string) {
	if ql.head == nil {
		ql.head = &Node{
			values: []string{value},
			next:   nil,
			prev:   nil,
		}
		ql.tail = ql.head
	} else if len(ql.tail.values) < ql.maxNode {
		ql.tail.values = append(ql.tail.values, value)

	} else {
		newNode := &Node{
			values: []string{value},
			next:   nil,
			prev:   ql.tail,
		}
		ql.tail.next = newNode
		ql.tail = newNode
	}
	ql.length++
}

func lpop(args []Value) Value {
	if len(args) > 1 {
		return Value{typ: "error", str: "ERROR : wrong number of argumments for 'lpop' command"}
	}
	key := args[0].bulk
	Listsmu.Lock()
	defer Listsmu.Unlock()

	_, ok := LISTs[key]

	if !ok {

		return Value{typ: "error", str: fmt.Sprintf("ERROR : Invallid list name %s", key)}

	}

	elem, ok := LISTs[key].LPop()

	if !ok {
		return Value{typ: "error", str: elem}
	}

	return Value{typ: "string", str: elem}

}
func (ql *QuickList) LPop() (string, bool) {
	if ql.length == 0 {
		return "Error: The list is empty", false
	}

	elem := ql.head.values[0]

	if len(ql.head.values) == 1 {
		ql.head = ql.head.next
		if ql.head != nil {
			ql.head.prev = nil
		} else {
			ql.tail = nil // list is now empty
		}
	} else {
		ql.head.values = ql.head.values[1:]
	}

	ql.length--
	return elem, true
}

func rpop(args []Value) Value {
	if len(args) > 1 {
		return Value{typ: "error", str: "ERROR : wrong number of argumments for 'lpop' command"}
	}
	key := args[0].bulk
	Listsmu.Lock()
	defer Listsmu.Unlock()

	_, ok := LISTs[key]

	if !ok {

		return Value{typ: "error", str: fmt.Sprintf("ERROR : Invallid list name %s", key)}

	}

	elem, ok := LISTs[key].RPop()

	if !ok {
		return Value{typ: "error", str: elem}
	}

	return Value{typ: "string", str: elem}

}

func (ql *QuickList) RPop() (string, bool) {
	if ql.length == 0 {
		return "Error: The list is empty", false
	}

	elem := ql.tail.values[len(ql.tail.values)-1]

	if len(ql.tail.values) == 1 {

		if ql.tail.prev == nil {
			ql.head = nil
			ql.tail = nil
		} else {
			ql.tail = ql.tail.prev
			ql.tail.next = nil
		}
	} else {
		ql.tail.values = ql.tail.values[:len(ql.tail.values)-1]
	}

	ql.length--
	return elem, true
}

func llen(args []Value) Value {
	if len(args) > 1 {
		return Value{typ: "error", str: "ERROR : wrong number of argumments for 'llen' command"}

	}

	key := args[0].bulk
	Listsmu.RLock()
	defer Listsmu.RUnlock()
	_, ok := LISTs[key]

	if !ok {
		return Value{typ: "error", str: "0"}

	}

	return Value{typ: "string", str: fmt.Sprintf("%d", LISTs[key].length)}
}

func lrange(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERROR : wrong number of argumments for 'lrange' command"}

	}
	key := args[0].bulk
	start := args[1].bulk
	end := args[2].bulk

	Listsmu.RLock()
	defer Listsmu.RUnlock()

	i64Start, err := strconv.ParseInt(string(start), 10, 64)
	i64End, err2 := strconv.ParseInt(string(end), 10, 64)

	if err != nil || err2 != nil {
		return Value{typ: "error", str: "ERROR : Invalid argumments for 'lrange' command"}

	}

	ql, ok := LISTs[key]
	if !ok {
		return Value{typ: "array", arr: []Value{}}
	}

	n := ql.length
	if n == 0 {
		return Value{typ: "array", arr: []Value{}}
	}

	if i64Start < 0 {
		i64Start = int64(n) + i64Start
	}
	if i64End < 0 {
		i64End = int64(n) + i64End
	}

	if i64Start < 0 {
		i64Start = 0
	}
	if i64End < 0 {
		return Value{typ: "array", arr: []Value{}}
	}
	if i64Start > i64End {
		return Value{typ: "array", arr: []Value{}}
	}
	if i64Start >= int64(n) {
		return Value{typ: "array", arr: []Value{}}
	}
	if i64End >= int64(n) {
		i64End = int64(n - 1)
	}

	result := []Value{}
	temp := ql.head
	idx := 0

	for temp != nil && idx <= int(i64End) {

		for _, v := range temp.values {
			if int64(idx) >= i64Start && int64(idx) <= i64End {
				result = append(result, Value{typ: "string", str: v})
			}
			idx++
			if int64(idx) > i64End {
				break
			}
		}
		temp = temp.next

	}

	return Value{typ: "array", arr: result}
}

func lindex(args []Value) Value {

	if len(args) != 2 {
		return Value{typ: "error", str: "ERROR : Invalid argumments for 'lindex' command"}
	}

	key := args[0].bulk
	idx, err := strconv.ParseInt(args[1].bulk, 10, 64)

	if err != nil {
		return Value{typ: "error", str: "ERROR : Invalid argumments for 'lindex' command"}

	}

	Listsmu.RLock()
	defer Listsmu.RUnlock()

	ql, ok := LISTs[key]

	if !ok {
		return Value{typ: "null"}
	}

	if idx < 0 {
		idx = idx + int64(ql.length)
	}
	if idx < 0 {
		return Value{typ: "null"}
	}

	if int64(ql.length) < idx {
		return Value{typ: "null"}
	}

	temp := ql.head
	var idx2 int64 = 0

	for temp != nil {

		for _, v := range temp.values {
			if idx2 == idx {
				return Value{typ: "string", str: v}
			}
			idx2++
		}
		temp = temp.next

	}

	return Value{typ: "null"} //UNREACHABLE but for satisfying compiler

}

func lset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERROR : Invalid argumments for 'lset' command"}
	}

	key := args[0].bulk
	idx, err := strconv.ParseInt(args[1].bulk, 10, 64)

	if err != nil {
		return Value{typ: "error", str: "ERROR : Invalid argumments for 'lset' command"}
	}
	value := args[2].bulk

	Listsmu.RLock()
	defer Listsmu.RUnlock()

	ql, ok := LISTs[key]

	if !ok {
		return Value{typ: "null"}

	}

	if idx < 0 {
		idx = idx + int64(ql.length)
	}
	if idx < 0 || idx >= int64(ql.length) {
		return Value{typ: "error", str: "Index out of range"}
	}
	temp := ql.head
	var idx2 int64 = 0

	for temp != nil {
		for i := 0; i < len(temp.values); i++ {
			if idx2 == idx {
				temp.values[i] = value
				return Value{typ: "string", str: "OK"}
			}
			idx2++
		}
		temp = temp.next
	}

	// should never reach here
	return Value{typ: "error", str: "Index not found"}

}
