# Concurrent Key-Value Store (Jmap)

A custom, high-performance Concurrent HashMap implementation in Java with Time-To-Live (TTL) support, extensive concurrency strategies, and a built-in interactive Command Line Interface.

## Features

- **Custom Concurrent Map (`Jmap`)**: Implements a highly concurrent hash map using bucket-level `ReentrantReadWriteLock`s to minimize thread contention down to the specific bucket level.
- **Optimistic Reads**: Utilizes Java's `StampedLock` for fast optimistic reads, scaling up throughput. When validation fails (like during a concurrent resize), it gracefully falls back to secure read locks.
- **Dynamic Resizing**: Automatically handles resizing and re-hashing with minimal disruption when the load factor threshold is reached (default 0.75).
- **Time-To-Live (TTL) Expiration**: Attach an expiration time to any key using the `putexp` command. A background `TTLManager` daemon thread efficiently cleans up expired keys using a `DelayQueue`.

## Project Structure

- `src/main/java/cache/Jmap.java`: The core concurrent hash map implementation, featuring fine-grained locking and optimistic reads.
- `src/main/java/cache/Jnode.java`: Represents a node/bucket entry in the map.
- `src/main/java/cache/KVStore.java`: The main application class providing the interactive CLI.
- `src/main/java/cache/TTLManager.java`: Manages key expiration and cleanup using a background daemon thread.
- `src/main/java/cache/ExpiryEntry.java`: A supporting class used by `DelayQueue` to track the expiration time of keys.
- `src/main/java/cache/JmapBenchmark.java`: JMH benchmarking setup to compare throughput running with 1, 8, and 16 concurrent threads.
- `src/main/java/cache/StressTest.java`: Multi-threaded integration tests running intensive concurrent operations.
- `src/test/java/cache/TTLTest.java`: Unit tests ensuring correct TTL behavior.

## Usage

You can run the interactive CLI by executing the `KVStore` main class.

### CLI Commands

- `put <key> <value>`: Insert or update a key with a value.
- `putexp <key> <value> <ttl_seconds>`: Insert or update a key with a value and a Time-To-Live (TTL) in seconds.
- `get <key>`: Retrieve the value associated with a key.
- `del <key>` (or `remove <key>`): Delete a key from the store.
- `show` (or `display`): Print the internal state and structure of the map.
- `count` (or `size`): Print the total number of non-expired nodes in the map.
- `help`: Display the list of available commands.
- `exit` (or `quit`): Safely shut down the CLI and background threads.

## Build and Run

This project uses Maven for dependency management and builds. Built on Java 21, packing standard tools like JUnit 5 and JMH.

```bash
mvn clean compile
mvn exec:java -Dexec.mainClass="cache.KVStore"
```

### Running Benchmarks

Because the program implements JMH benchmarks, you can package and run them locally to observe throughput comparison against `java.util.concurrent.ConcurrentHashMap`:

```bash
mvn clean install
java -jar target/benchmarks.jar
```
Global ReentrantReadWriteLock became a bottleneck as thread count increased. Although multiple readers can hold the read lock concurrently, acquiring and releasing the read lock is not free. Internally, the lock maintains shared state (such as reader counts) that is updated atomically. This lock state resides in a cache line that may be cached by multiple CPU cores. When a thread acquires or releases the read lock, its core must obtain exclusive ownership of that cache line before modifying it. This causes cache coherence traffic as ownership of the cache line moves between cores. Under high read concurrency, many threads contend on the same lock metadata, causing frequent cache-line bouncing and reducing scalability. As thread count increases, the overhead of maintaining the shared reader count becomes significant even though the protected data itself is only being read.
Future scope:

Might move fom one single global lock to sharding the entire map into smaller pieces and having locks for those smaller shards because stampedlock doesn't always try to acquire a lock but it can also still fail when no. of writes are more.