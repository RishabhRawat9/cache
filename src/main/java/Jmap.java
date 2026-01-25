import java.security.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Jmap<K, V> {

    private Jnode<K, V>[] table;
    private final AtomicInteger size = new AtomicInteger(16);
    private float resize_threshold = 0.75f; // so when 75% of the capacity is filled i resize the thing right;
    public static volatile AtomicInteger node_ct = new AtomicInteger(0);

    private ReadWriteLock[] rwLocks;
    private ReadWriteLock globalRwLock = new ReentrantReadWriteLock();
    private Lock resizeLock = globalRwLock.writeLock(); // only acquire this at the time of resizing;
    private Lock readLock = globalRwLock.readLock(); // only acquire this at the time of resizing;

    @SuppressWarnings("unchecked")
    public Jmap() {
        this.table = (Jnode<K, V>[]) new Jnode[size.get()];
        this.rwLocks = new ReentrantReadWriteLock[size.get()];
        for (int j = 0; j < size.get(); j++) {
            rwLocks[j] = new ReentrantReadWriteLock();
        }
    }

    @SuppressWarnings("unchecked")
    public Jmap(float threshold) {
        this.table = (Jnode<K, V>[]) new Jnode[size.get()];
        this.resize_threshold = threshold;
        this.rwLocks = new ReentrantReadWriteLock[size.get()];
        for (int j = 0; j < size.get(); j++) {
            rwLocks[j] = new ReentrantReadWriteLock();
        }
    }

    // now every time a new node is placed i check if resizing is required or not;

    private int hashFunction(K key, int mapCapacity) {
        // so when a key is provided it returns a hash value;
        int h = Math.abs(key.hashCode());
        //same as concurrentHahsMap;
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);
        return h & (mapCapacity - 1);
    }

    // how do i efficiently put a value with ttl?
    // do i overload the method but that would result in duplicate code;
    //do i put a condition

    public void put(K key, V value) {
        readLock.lock();
        int tableIndex = hashFunction(key, size.get());
        ReadWriteLock bucketLock = rwLocks[tableIndex];
        bucketLock.writeLock().lock();
        try {
            boolean newNodePlaced = false;
            Jnode<K, V> node = new Jnode<>(tableIndex, key, value);

            // System.out.println(Thread.currentThread().getName() + " got lock " + tableIndex);
            if (table[tableIndex] == null) {
                table[tableIndex] = node;
                node_ct.getAndIncrement();
                newNodePlaced = true;
            } else {
                Jnode<K, V> currHeadNode = table[tableIndex];
                while (currHeadNode != null) {
                    if (currHeadNode.key.equals(key)) {
                        currHeadNode.value = value;
                        currHeadNode.ttl = -1;
                        return;
                    } else {
                        if (currHeadNode.next != null) {
                            currHeadNode = currHeadNode.next;
                        } else {
                            currHeadNode.next = node;
                            newNodePlaced = true;
                            node_ct.getAndIncrement();
                            break;
                        }
                    }
                }
            }
            if (newNodePlaced) {
                boolean resizeRequired = (node_ct.get() >= size.get() * resize_threshold);
                if (resizeRequired) {
                    readLock.unlock();
                    bucketLock.writeLock().unlock();
                    resizeLock.lock(); // checking again if resize is required
                    try {
                        if ((node_ct.get() >= size.get() * resize_threshold)) {
                            // System.out.println("resize triggereed");
                            resizeJmap();
                        }
                    } finally {
                        resizeLock.unlock();
                        readLock.lock(); // reacquired this because of the release outside;
                        bucketLock.writeLock().lock();
                    }
                }
            }
        } finally {
            bucketLock.writeLock().unlock();
            readLock.unlock();
        }
    }

    //put/putexp on an existing key overwrites the ttl of the prev key;
    public void putexp(K key, V value, long ttl) {
        readLock.lock();
        int tableIndex = hashFunction(key, size.get());
        ReadWriteLock bucketLock = rwLocks[tableIndex];
        bucketLock.writeLock().lock();
        try {
            boolean newNodePlaced = false;
            Jnode<K, V> node = new Jnode<>(tableIndex, key, value, ttl);

            // System.out.println(Thread.currentThread().getName() + " got lock " + tableIndex);
            if (table[tableIndex] == null) {
                table[tableIndex] = node;
                node_ct.getAndIncrement();
                newNodePlaced = true;
            } else {
                Jnode<K, V> currHeadNode = table[tableIndex];
                while (currHeadNode != null) {
                    if (currHeadNode.key.equals(key)) {
                        currHeadNode.value = value;
                        currHeadNode.ttl = System.currentTimeMillis() + ttl;
                        return;
                    } else {
                        if (currHeadNode.next != null) {
                            currHeadNode = currHeadNode.next;
                        } else {
                            currHeadNode.next = node;
                            newNodePlaced = true;
                            node_ct.getAndIncrement();
                            break;
                        }
                    }
                }
            }
            if (newNodePlaced) {
                boolean resizeRequired = (node_ct.get() >= size.get() * resize_threshold);
                if (resizeRequired) {
                    readLock.unlock();
                    bucketLock.writeLock().unlock();
                    resizeLock.lock(); // checking again if resize is required
                    try {
                        if ((node_ct.get() >= size.get() * resize_threshold)) {
                            // System.out.println("resize triggereed");
                            resizeJmap();
                        }
                    } finally {
                        resizeLock.unlock();
                        readLock.lock(); // reacquired this because of the release outside;
                        bucketLock.writeLock().lock();
                    }
                }
            }
        } finally {
            bucketLock.writeLock().unlock();
            readLock.unlock();
        }
    }

    public void remove(K key) {
        readLock.lock();
        int tableIndex = hashFunction(key, size.get());
        ReadWriteLock bucketLock = rwLocks[tableIndex];
        bucketLock.writeLock().lock();
        try {
            Jnode<K, V> current = table[tableIndex];
            Jnode<K, V> prev = null;
            if (current == null) {
                throw new IllegalArgumentException("Key not found: " + key);
            }
            while (current != null && !current.key.equals(key)) {
                prev = current;
                current = current.next;
            }
            if (current == null) {
                throw new IllegalArgumentException("Key not found: " + key);
            }
            if (prev == null) {
                // if it's the first node only;;
                table[tableIndex] = current.next;
            } else {
                prev.next = current.next; // deleted the curr one gc removes it;
            }

            node_ct.decrementAndGet();
        } finally {
            bucketLock.writeLock().unlock();
            readLock.unlock();
        }
    }

    public V get(K key) {
        // so this is a read i just need to acquire the global read lock and the bucket
        // level read lock;
        // blocks incase a resize is happening;
        readLock.lock();
        int tableIndex = hashFunction(key, size.get());
        ReadWriteLock bucketLock = rwLocks[tableIndex];
        bucketLock.readLock().lock();
        // prevents reads and writes on same bucket at the saem time , and it allows
        // multiple reads to happen together, previously reads would've blocked.
        try {
            Jnode<K, V> value = table[tableIndex];
            Jnode<K, V> prev = null;
            while (value != null) {
                if (value.key.equals(key) && (value.ttl == -1 || System.currentTimeMillis() <= value.ttl)) {
                    return value.value;
                } else if (value.key.equals(key) && value.ttl != -1 && System.currentTimeMillis() > value.ttl) {
                    //will have to manually remove the entry, can't use remove() bcoz it'll deadlock;
                    if (prev == null) {
                        table[tableIndex] = value.next;
                    } else {
                        prev.next = value.next;
                    }
                    System.out.println("removed old entry:" + value.key + " " + value.value);
                    return null;
                } else {
                    prev = value;
                    value = value.next;
                }
            }
            return null;
        } finally {
            /// always executes;
            bucketLock.readLock().unlock();
            readLock.unlock();
        }
    }

    public void resize_put(K key, V value, Jnode<K, V>[] newTable) {
        int tableIndex = hashFunction(key, newTable.length);
        Jnode<K, V> node = new Jnode<>(tableIndex, key, value);
        if (newTable[tableIndex] == null) {
            newTable[tableIndex] = node;
            node_ct.getAndIncrement();
        } else {
            Jnode<K, V> currHeadNode = newTable[tableIndex];
            while (currHeadNode != null) {
                if (currHeadNode.next != null) {
                    currHeadNode = currHeadNode.next;
                } else {
                    currHeadNode.next = node;
                    node_ct.getAndIncrement();
                    return;
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void resizeJmap() {
        // so a completely new array with twice the size is required now
        int newSize = size.get() * 2;
        node_ct.set(0);
        Jnode<K, V>[] newTable = (Jnode<K, V>[]) new Jnode[newSize];
        for (int j = 0; j < size.get(); j++) {
            Jnode<K, V> currNode = table[j];

            while (currNode != null) {
                resize_put(currNode.key, currNode.value, newTable);
                currNode = currNode.next;
            }
        }

        this.table = newTable;
        this.size.set(newSize);
        this.rwLocks = new ReentrantReadWriteLock[size.get()];
        for (int j = 0; j < size.get(); j++) {
            rwLocks[j] = new ReentrantReadWriteLock();
        }
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();

        for (int j = 0; j < size.get(); j++) {
            if (table[j] != null) {
                Jnode<K, V> tempNode = table[j];
                String formattedString = String.format("[%d] ->", j);
                str.append(formattedString);
                while (tempNode != null) {
                    // append every node to str;
                    str.append(tempNode).append("->");
                    tempNode = tempNode.next;
                }
                str.append("\n");
            } else {
                str.append("[").append(j).append("]->").append("null").append("\n");
            }
        }
        return str.toString();
    }
}
