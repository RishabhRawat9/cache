import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TTLTest {
    
    public static void main(String[] args) throws InterruptedException {
        testBasicTTL();
        testMultipleTTLs();
        testTTLUpdateScenario();
        testConcurrentTTLOperations();
        System.out.println("All TTL tests passed!");
    }
    
    /**
     * Test basic TTL expiration
     */
    public static void testBasicTTL() throws InterruptedException {
        System.out.println("\n=== Testing Basic TTL ===");
        Jmap<String, String> map = new Jmap<>(0.75f);
        TTLManager ttlManager = new TTLManager(map);
        
        // Put key with 2 second TTL
        long expiryTime = System.currentTimeMillis() + 2000;
        map.putexp("testkey", "testvalue", 2000);
        ttlManager.schedule("testkey", expiryTime);
        
        // Should exist immediately
        assert map.get("testkey").equals("testvalue") : "Key should exist immediately";
        System.out.println("✓ Key exists immediately after putexp");
        
        // Wait 1 second - should still exist
        Thread.sleep(1000);
        assert map.get("testkey").equals("testvalue") : "Key should exist after 1s";
        System.out.println("✓ Key exists after 1s");
        
        // Wait 2 more seconds - should be expired
        Thread.sleep(2500);
        assert map.get("testkey") == null : "Key should be expired after 3.5s";
        System.out.println("✓ Key expired after 3.5s");
        
        ttlManager.shutdown();
    }
    
    /**
     * Test multiple keys with different TTLs
     */
    public static void testMultipleTTLs() throws InterruptedException {
        System.out.println("\n=== Testing Multiple TTLs ===");
        Jmap<String, String> map = new Jmap<>(0.75f);
        TTLManager ttlManager = new TTLManager(map);
        
        long now = System.currentTimeMillis();
        
        // Key1: 1 second TTL
        map.putexp("key1", "value1", 1000);
        ttlManager.schedule("key1", now + 1000);
        
        // Key2: 3 second TTL
        map.putexp("key2", "value2", 3000);
        ttlManager.schedule("key2", now + 3000);
        
        // Key3: 5 second TTL
        map.putexp("key3", "value3", 5000);
        ttlManager.schedule("key3", now + 5000);
        
        // All should exist initially
        assert map.get("key1") != null && map.get("key2") != null && map.get("key3") != null;
        System.out.println("✓ All keys exist initially");
        
        // After 1.5s: key1 expired, key2 and key3 exist
        Thread.sleep(1500);
        assert map.get("key1") == null : "key1 should be expired";
        assert map.get("key2") != null : "key2 should exist";
        assert map.get("key3") != null : "key3 should exist";
        System.out.println("✓ key1 expired, key2 and key3 exist after 1.5s");
        
        // After 3.5s total: key1 and key2 expired, key3 exists
        Thread.sleep(2000);
        assert map.get("key1") == null : "key1 should be expired";
        assert map.get("key2") == null : "key2 should be expired";
        assert map.get("key3") != null : "key3 should exist";
        System.out.println("✓ key1 and key2 expired, key3 exists after 3.5s");
        
        // After 5.5s total: all expired
        Thread.sleep(2000);
        assert map.get("key1") == null && map.get("key2") == null && map.get("key3") == null;
        System.out.println("✓ All keys expired after 5.5s");
        
        ttlManager.shutdown();
    }
    
    /**
     * Test TTL update scenario - old entry in queue should be ignored
     */
    public static void testTTLUpdateScenario() throws InterruptedException {
        System.out.println("\n=== Testing TTL Update Scenario ===");
        Jmap<String, String> map = new Jmap<>(0.75f);
        TTLManager ttlManager = new TTLManager(map);
        
        long now = System.currentTimeMillis();
        
        // Initial: 2 second TTL
        map.putexp("updatekey", "value1", 2000);
        ttlManager.schedule("updatekey", now + 2000);
        
        // Wait 1 second, then update with 4 second TTL from now
        Thread.sleep(1000);
        long newExpiry = System.currentTimeMillis() + 4000;
        map.putexp("updatekey", "value2", 4000);
        ttlManager.schedule("updatekey", newExpiry);
        
        // After original 2s would have expired, key should still exist
        Thread.sleep(1500); // 2.5s total
        assert map.get("updatekey") != null : "Key should still exist after update";
        assert map.get("updatekey").equals("value2") : "Value should be updated";
        System.out.println("✓ Key exists after TTL update (stale entry ignored)");
        
        // Should expire after the new TTL
        Thread.sleep(3000); // 5.5s total
        assert map.get("updatekey") == null : "Key should be expired after new TTL";
        System.out.println("✓ Key expired after new TTL period");
        
        ttlManager.shutdown();
    }
    
    /**
     * Test concurrent TTL operations
     */
    public static void testConcurrentTTLOperations() throws InterruptedException {
        System.out.println("\n=== Testing Concurrent TTL Operations ===");
        Jmap<String, String> map = new Jmap<>(0.75f);
        TTLManager ttlManager = new TTLManager(map);
        
        int numKeys = 100;
        CountDownLatch latch = new CountDownLatch(numKeys);
        
        // Add multiple keys with TTL concurrently
        for (int i = 0; i < numKeys; i++) {
            final int keyNum = i;
            new Thread(() -> {
                try {
                    long ttl = 1000 + (keyNum % 3) * 1000; // 1-3 second TTL
                    long expiry = System.currentTimeMillis() + ttl;
                    map.putexp("concurrentkey" + keyNum, "value" + keyNum, ttl);
                    ttlManager.schedule("concurrentkey" + keyNum, expiry);
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        
        latch.await(); // Wait for all puts to complete
        System.out.println("✓ All concurrent puts completed");
        
        // Wait for all to expire
        Thread.sleep(4500);
        
        // Check all are expired
        int existingCount = 0;
        for (int i = 0; i < numKeys; i++) {
            if (map.get("concurrentkey" + i) != null) {
                existingCount++;
            }
        }
        
        assert existingCount == 0 : "All keys should be expired, but " + existingCount + " still exist";
        System.out.println("✓ All concurrent keys expired properly");
        
        ttlManager.shutdown();
    }
}