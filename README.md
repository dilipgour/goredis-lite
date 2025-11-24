# GoRedis-Lite 🚀

A **Redis-inspired in-memory database** implemented in **Go**.  
Supports lists with operations like LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, and LSET using a **QuickList data structure** for efficient storage and retrieval.

---

## Features

- **QuickList-based list storage**: Doubly-linked list of arrays for efficient inserts/deletes.
- **List commands implemented**:
  - `LPUSH` / `RPUSH` — Push elements to head/tail
  - `LPOP` / `RPOP` — Pop elements from head/tail
  - `LLEN` — Get list length
  - `LRANGE` — Get a range of elements
  - `LINDEX` — Get element at index
  - `LSET` — Update element at index
- **Thread-safe operations** using Go’s `sync.RWMutex`
- **Handles negative indices** like Redis
- **Amortized O(1)** insertion for list operations

---

## Installation

1. Clone the repository:

```bash
git clone git@github.com:dilipgour/goredis-lite.git
cd goredis-lite
