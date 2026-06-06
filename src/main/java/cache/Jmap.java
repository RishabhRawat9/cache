package cache;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

public class Jmap<K, V> {

    private volatile Jnode<K, V>[] table;
    private final AtomicInteger size = new AtomicInteger(16);
    private float resize_threshold = 0.75f;
    private final AtomicInteger node_ct = new AtomicInteger(0);

    private volatile ReadWriteLock[] rwLocks;
    private final StampedLock stampedLock = new StampedLock();

    public int getNodeCt() {
        return node_ct.get();
    }

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
        if (!(threshold > 0.0f) || Float.isNaN(threshold) || Float.isInfinite(threshold)) {
            throw new IllegalArgumentException("threshold must be a finite value > 0");
        }
        this.table = (Jnode<K, V>[]) new Jnode[size.get()];
        this.resize_threshold = threshold;
        this.rwLocks = new ReentrantReadWriteLock[size.get()];
        for (int j = 0; j < size.get(); j++) {
            rwLocks[j] = new ReentrantReadWriteLock();
        }
    }

    private int hashFunction(K key, int mapCapacity) {
        int h = Math.abs(key.hashCode());
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);
        return h & (mapCapacity - 1);
    }

    public void put(K key, V value) {
        //
        long stamp = stampedLock.readLock();
        int tableIndex = hashFunction(key, size.get());
        ReadWriteLock bucketLock = rwLocks[tableIndex];
        bucketLock.writeLock().lock();
        boolean needResize = false;
        try {
            boolean newNodePlaced = false;
            Jnode<K, V> node = new Jnode<>(tableIndex, key, value);

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
            needResize = newNodePlaced && (node_ct.get() >= size.get() * resize_threshold);
        } finally {
            bucketLock.writeLock().unlock();
            stampedLock.unlockRead(stamp);
        }

        if (needResize) {
            long writeStamp = stampedLock.writeLock();
            try {
                if (node_ct.get() >= size.get() * resize_threshold) {
                    resizeJmap();
                }
            } finally {
                stampedLock.unlockWrite(writeStamp);
            }
        }
    }

    public void putexp(K key, V value, long ttl) {
        long stamp = stampedLock.readLock();
        int tableIndex = hashFunction(key, size.get());
        ReadWriteLock bucketLock = rwLocks[tableIndex];
        bucketLock.writeLock().lock();
        boolean needResize = false;
        try {
            boolean newNodePlaced = false;
            Jnode<K, V> node = new Jnode<>(tableIndex, key, value, ttl);

            if (table[tableIndex] == null) {
                table[tableIndex] = node;
                node_ct.getAndIncrement();
                newNodePlaced = true;
            } else {
                Jnode<K, V> currHeadNode = table[tableIndex];
                while (currHeadNode != null) {
                    if (currHeadNode.key.equals(key)) {
                        currHeadNode.value = value;
                        currHeadNode.ttl = ttl;
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
            needResize = newNodePlaced && (node_ct.get() >= size.get() * resize_threshold);
        } finally {
            bucketLock.writeLock().unlock();
            stampedLock.unlockRead(stamp);
        }

        if (needResize) {
            long writeStamp = stampedLock.writeLock();
            try {
                if (node_ct.get() >= size.get() * resize_threshold) {
                    resizeJmap();
                }
            } finally {
                stampedLock.unlockWrite(writeStamp);
            }
        }
    }

    public void remove(K key) {
        long stamp = stampedLock.readLock();
        int tableIndex = hashFunction(key, size.get());
        ReadWriteLock bucketLock = rwLocks[tableIndex];
        bucketLock.writeLock().lock();
        try {
            Jnode<K, V> current = table[tableIndex];
            Jnode<K, V> prev = null;
            if (current == null)
                return;

            while (current != null && !current.key.equals(key)) {
                prev = current;
                current = current.next;
            }
            if (current == null)
                return;

            if (prev == null) {
                table[tableIndex] = current.next;
            } else {
                prev.next = current.next;
            }
            node_ct.decrementAndGet();
        } finally {
            bucketLock.writeLock().unlock();
            stampedLock.unlockRead(stamp);
        }
    }

    public V get(K key) {
        while (true) {
            long stamp = stampedLock.tryOptimisticRead();
            int tableIndex = hashFunction(key, size.get());
            Jnode<K, V>[] localTable = table;
            ReadWriteLock[] localLocks = rwLocks;
            boolean optimisticReadFailed = false;
            // if the the validation fails it means that resizing happened because the
            // stampled write lock can only cause our stamp to be invalidated;

            if (!stampedLock.validate(stamp)) {
                stamp = stampedLock.readLock();// now we try with a readlock to make sure that a resize doesn't happen
                                               // while we are trying to acquire the bucket lock;
                optimisticReadFailed = true;
                tableIndex = hashFunction(key, size.get());
                localTable = table;
                localLocks = rwLocks;
            } // the resize has caused the buckets to be changed;

            ReadWriteLock bucketLock = localLocks[tableIndex];
            bucketLock.readLock().lock();

            // resize happened between our optimistic validate and bucket lock acquisition
            if (!optimisticReadFailed && !stampedLock.validate(stamp)) {
                bucketLock.readLock().unlock();
                continue;
            }

            try {
                Jnode<K, V> node = localTable[tableIndex];
                while (node != null) {
                    if (node.key.equals(key) && (node.ttl == -1 || System.currentTimeMillis() <= node.ttl)) {
                        return node.value;
                    } else if (node.key.equals(key) && node.ttl != -1 && System.currentTimeMillis() > node.ttl) {
                        return null;
                    } else {
                        node = node.next;
                    }
                }
                return null;
            } finally {
                bucketLock.readLock().unlock();
                if (optimisticReadFailed)
                    stampedLock.unlockRead(stamp);
            }
        }
    }

    public Long getExpiry(K key) {
        while (true) {
            long stamp = stampedLock.tryOptimisticRead();
            int tableIndex = hashFunction(key, size.get());
            Jnode<K, V>[] localTable = table;
            ReadWriteLock[] localLocks = rwLocks;
            boolean optimisticReadFailed = false;

            if (!stampedLock.validate(stamp)) {
                stamp = stampedLock.readLock();
                optimisticReadFailed = true;
                tableIndex = hashFunction(key, size.get());
                localTable = table;
                localLocks = rwLocks;
            }

            ReadWriteLock bucketLock = localLocks[tableIndex];
            bucketLock.readLock().lock();

            if (!optimisticReadFailed && !stampedLock.validate(stamp)) {
                bucketLock.readLock().unlock();
                continue;
            }

            try {
                Jnode<K, V> node = localTable[tableIndex];
                while (node != null) {
                    if (node.key.equals(key)) {
                        return node.ttl == -1 ? null : node.ttl;
                    }
                    node = node.next;
                }
                return null;
            } finally {
                bucketLock.readLock().unlock();
                if (optimisticReadFailed)
                    stampedLock.unlockRead(stamp);
            }
        }
    }

    public void resize_put(Jnode<K, V> node, Jnode<K, V>[] newTable) {
        int tableIndex = hashFunction(node.key, newTable.length);
        node.next = newTable[tableIndex];
        newTable[tableIndex] = node;
    }

    @SuppressWarnings("unchecked")
    private void resizeJmap() {
        int newSize = size.get() * 2;
        Jnode<K, V>[] newTable = (Jnode<K, V>[]) new Jnode[newSize];
        for (int j = 0; j < size.get(); j++) {
            Jnode<K, V> currNode = table[j];
            while (currNode != null) {
                Jnode<K, V> nextNode = currNode.next;
                if (currNode.ttl != -2) {
                    resize_put(currNode, newTable);
                }
                currNode = nextNode;
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
                str.append(String.format("[%d] ->", j));
                while (tempNode != null) {
                    str.append(tempNode).append("->");
                    tempNode = tempNode.next;
                }
                str.append("\n");
            } else {
                str.append("[").append(j).append("]->null\n");
            }
        }
        return str.toString();
    }
}