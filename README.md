# PirinDB

<img src="images/pirindb_logo.png" alt="PirinDB Logo" width="200">

**PirinDB** is a lightweight key-value database developed as a personal project to deepen my 
understanding of database internals.  
Storage engine is partial based on the implementation of [**LibraDB**](https://github.com/amit-davidson/LibraDB), 
following the excellent article, and draws further inspiration from Alex Petrov’s [book](https://www.amazon.com/Database-Internals-Deep-Distributed-Systems/dp/1492040347) 
and [**BoltDB**](https://github.com/boltdb/bolt) internals. **PirinDB** provides an approachable way to explore how B+Tree-based storage engines
work under the hood.

Another goal of the project is to develop database server with simple HTTP API, with sharding and 
replication support, to play with concepts of distributed systems.

Project contains:
 - Storage engine with BoltDB-like API
 - Database server
 - CLI client

## Features

**Storage Engine Features:**

- **Small Codebase:** Designed to be easily readable.
- **BoltDB like API:** Easy to integrate and experiment with.
- **Persistence:** Data is stored on disk.
- **Simple Transactional Support:** Ensures data consistency.
- **Memory-mapped Storage:** Uses Unix `mmap` for efficient access and dynamically expands the storage file.
- **Dynamic Freelist Management:** Automatic allocation and reuse of storage pages.
- **Collection Management:** Tools for organizing and managing data collections.
- **Basic Cursor and Range Scanning:** Provides mechanisms to iterate over data ranges efficiently.
- **View/Update Syntax Sugar:** Convenient API methods to simplify database interactions.
- [x] **Blob Storage:** Supports for BLOBs.
- [ ] **Copy on-write:** Support for copy-on-write for data consistency and recovery.
- [ ] **Simple ORM:** Basic object-relational mapping layer for data structures.
- [ ] **Statistics:** Built-in statistics.

**Database server:**

- [x] Simple HTTP server
- [x] Basic key-value operations
- [ ] Multi-key operations
- [ ] Range scanning operations
- [ ] Sharding support (naive or consistent hashing)
- [ ] Replication support

## Project Status

**PirinDB** is intended purely as a hobby and educational project. It is not designed to be used  
as a production-grade database. Instead, it's a playground for those interested in learning and  
exploring the inner workings of database management systems.

[!WARNING]
The current implementation does **not** include copy-on-write (COW) mechanisms.  
If a transaction fails during the process of writing pages or updating the freelist, it may lead 
to data corruption.

## Quick Start
You need Go 1.22 or later to build PirinDB.

To build PirinDB server and client, run:

```bash
$ make build
```
It will create two binaries in the `bin` directory: `pirindb` and `pirindb-cli`.

To start the server, run:

```bash
$ ./bin/pirindb
```
It will run the server on port 4321, and you can access the server at `http://localhost:4321`.
Database file (by default **pirin.db**) is stored in the current directory.

To start the CLI client, run:

```bash
$ ./bin/pirindb-cli
```

CLI client provides a simple interface to interact with the server. It supports the following commands:
- `get <key>`: Retrieves the value for the provided key.
- `set <key> <value>`: Sets the value for the provided key.
- `delete <key>`: Deletes the key-value pair.
- `status`: Retrieves the server status.
- `help`: Displays the help message.



## Storage API Quick Start

To start using pirindb storage engine, you need install the package:

```bash
$ go get github.com/timson/pirindb/storage/...
```

```Go
package main

import (
	"log"

	pirindb "github.com/timson/pirindb/storage"
)

func main() {
	db, err := pirindb.Open("test.db")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()
}

```

### Modify and Read Data

```Go
db.Update(func(tx *pirindb.Tx) error {
	bucket := tx.CreateBucket([]byte("foo"))
    if err := bucket.Put([]byte("foo"), []byte("bar")); err != nil {
        return err
    }
    return nil
})

db.View(func(tx *pirindb.Tx) error {)
    bucket := tx.GetBucket([]byte("foo"))
    value := bucket.Get([]byte("foo"))
    log.Println(string(value))
    return nil
})
```
[!NOTE]
If key value exceed the 1024 bytes, if automatically stored as a blob.


### Manual transaction management

```Go
tx := db.Begin(true)
defer tx.Rollback()
bucket := tx.CreateBucket([]byte("foo"))
bucket.Put([]byte("foo"), []byte("bar"))
tx.Commit()
```

### Cursors

Support for iterating over key-value pairs using cursors:
- `First()`: Move to the first key-value pair.
- `Next()`: Move to the next key-value pair.
- `Seek()`: Move to the first key-value pair that is greater than or equal to the provided key.
- `Last()`: Move to the last key-value pair.
- `Prev()`: Move to the previous key-value pair.

```Go
db.View(func(tx *pirindb.Tx) error {
    bucket := tx.GetBucket([]byte("foo"))
    cursor := bucket.Cursor()
    for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
        log.Printf("key: %s, value: %s", k, v)
    }
    return nil
})
```
Range scanning is also supported:

```Go
db.View(func(tx *pirindb.Tx) error {
    bucket := tx.GetBucket([]byte("foo"))
    cursor := bucket.Cursor()
    prefix := []byte("test")
    for k, v := cursor.Seek([]byte("bar")); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
        log.Printf("key: %s, value: %s", k, v)
    }
    return nil
})
```

[!NOTE]
Next() and Prev() methods works correctly only if the cursor is positioned on a valid key-value pair using 
First(), Last() or Seek().

### Bucket Management

PirinDB provides a simple mechanism for managing bucket of data.

- `CreateBucket()`: Creates a new bucket with the provided name.
- `GetBucket()`: Retrieves an existing bucket by name.
- `DeleteBucket()`: Deletes a bucket by name.
- `Buckets()`: Returns a list of all buckets in the database.



## Inspiration and Credits

- [BoltDB](https://github.com/boltdb/bolt)
- [LibraDB](https://github.com/amit-davidson/LibraDB)
- [Database Internals](https://www.amazon.com/Database-Internals-Deep-Distributed-Systems/dp/1492040347) by Alex Petrov

PirinDB was named after the Pirin Mountains. 
The [Pirin](https://en.wikipedia.org/wiki/Pirin) Mountains are a mountain 
range in southwestern Bulgaria.