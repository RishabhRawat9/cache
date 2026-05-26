# Custom HashMap

This project is a custom implementation of a thread-safe HashMap in Java. It is designed to be a learning exercise to understand the internal workings of a HashMap and multithreading in java.

## Features

- **Thread-Safe**: The `Jmap` class is thread-safe. It uses bucket-level locking to allow multiple threads to access the map concurrently. This is more efficient than locking the entire map for every operation.
- **Automatic Resizing**: The map automatically resizes itself when the number of elements exceeds the load factor. This is done to maintain a constant time complexity for the basic operations like `put`, `get`, and `remove`.
- **Key-Value Store**: The `KVStore` class provides a simple key-value store that uses the `Jmap` as its underlying data structure. It provides an interactive command-line interface to perform `put`, `get`, and `delete` operations.
- **Persistence**: The `KVStore` class also provides a simple persistence mechanism. It logs all the `put` and `delete` operations to a log file. When the application is started, it rebuilds the in-memory key-value store from the log file.

## Methods

- `PUT`: To add a new key-value pair.
- `GET`: To retrieve the value for a given key.
- `DELETE`: To remove a key-value pair.


mvn exec:java -Dexec.mainClass="KVStore"

## Known Issues

- **Thread Safety Violation in `get()`**: Modifies the linked list with only a read lock when lazily removing expired keys.
- **Count Inconsistency**: When `get()` dynamically removes expired keys, it fails to decrement `node_ct`.
- **Stale Lock Reference After Resize**: The `put` & `putexp` methods reacquire an old bucket lock (via a stale local variable) after a resize finishes, instead of looking up the new lock array.
- **Inconsistent TTL Calculation**: `putexp` applies absolute timestamps to updated entries, but relative TTLs to newly inserted entries (unless handled inside the `Jnode` constructor, which breaks encapsulation).
- **`resize_put` Encapsulation**: The method is marked `public` but skips bucket locking; it should be `private`.
- **No TTL Check on `remove()`**: Forced deletions behave identically for alive keys and logically expired ones.
