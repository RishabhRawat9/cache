import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class Jnode<K, V> {

    public int hash;
    public K key;
    public V value;
    public Jnode<K, V> next; // coz in the bucket we store nodes and in case of collision one bucket can have

    // multiple nodes so we store all of them using a linkedlist;
    long ttl = -1;

    public Jnode(int hashIndex, K key, V value) {
        this.hash = hashIndex;
        this.key = key;
        this.value = value;
        // this.ttl = System.currentTimeMillis() + ttl;
        this.next = null;
    }

    public Jnode(int hashIndex, K key, V value, long ttl) {
        this.hash = hashIndex;
        this.key = key;
        this.value = value;
        this.ttl = System.currentTimeMillis() + ttl;
        this.next = null;
    }

    @Override
    public String toString() {
        String val;
        if (value instanceof byte[]) {
            val = new String((byte[]) value, StandardCharsets.UTF_8);
        } else {
            val = String.valueOf(value);
        }
        return String.format("(%s, %s)", key, val);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Jnode)) return false;
        Jnode<?, ?> node = (Jnode<?, ?>) o;
        return Objects.equals(key, node.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }
}
