import java.util.concurrent.DelayQueue;

public class TTLManager<K, V> {
    // delayqueue is also a priorityqueue only but with default expiry for time and
    // the take() method blocking until an entry is past it's ttl that prevents me
    // from checking explicitly when to run the background thread to check the top
    // of the queue.;
    // delayqueue uses expiryEntry to check the delayed;
    private final DelayQueue<ExpiryEntry<K>> expiryQueue = new DelayQueue<>();
    private final Jmap<K, V> map;
    private volatile boolean running = true;
    private Thread cleanupThread;

    public TTLManager(Jmap<K, V> map) {
        this.map = map;
        startCleanupThread();
    }

    public void schedule(K key, long expiryTimeMillis) {
        expiryQueue.put(new ExpiryEntry<K>(key, expiryTimeMillis));
    }

    private void startCleanupThread() {
        cleanupThread = new Thread(() -> {
            while (running) {
                try {
                    // Blocks until an entry is ready to expire
                    ExpiryEntry<K> entry = expiryQueue.take();

                    // key might have been updated/deleted)
                    Long currentExpiry = map.getExpiry(entry.getKey());
                    if (currentExpiry != null && currentExpiry == entry.getExpiryTime()) {
                        try {
                            map.remove(entry.getKey());
                            System.out.println("[TTL] Expired and removed key: " + entry.getKey());
                        } catch (IllegalArgumentException e) {
                            // Key was already removed, ignore
                        }
                    }
                    // If expiry doesn't match, the key was updated — ignore stale entry

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "TTL-Cleanup-Thread");

        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }

    public void shutdown() {
        running = false;
        if (cleanupThread != null) {
            cleanupThread.interrupt();
        }
    }
}
