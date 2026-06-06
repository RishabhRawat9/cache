package cache;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class StressTest {

    static final int THREADS = 16;
    static final int OPS_PER_THREAD = 50_000;
    static final int KEY_SPACE = 2000; // will trigger multiple resizes
    static final int TTL_MS = 100;

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting full stress test...");
        Jmap<String, String> map = new Jmap<>();
        TTLManager<String, String> ttlManager = new TTLManager<>(map);

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch latch = new CountDownLatch(THREADS);
        AtomicInteger errors = new AtomicInteger(0);
        AtomicInteger corruptReads = new AtomicInteger(0);

        for (int t = 0; t < THREADS; t++) {
            pool.submit(() -> {
                try {
                    ThreadLocalRandom rng = ThreadLocalRandom.current();
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        int key = rng.nextInt(KEY_SPACE);
                        int op = rng.nextInt(10);

                        if (op < 3) {
                            // 30% plain put
                            map.put("key" + key, "val" + key);
                        } else if (op < 5) {
                            // 20% putexp
                            long expiry = System.currentTimeMillis() + TTL_MS;
                            map.putexp("key" + key, "val" + key, TTL_MS);
                            ttlManager.schedule("key" + key, expiry);
                        } else if (op < 8) {
                            // 30% get with correctness check
                            String val = map.get("key" + key);
                            if (val != null && !val.equals("val" + key)) {
                                corruptReads.incrementAndGet();
                                System.out.println("CORRUPTION: key" + key + " -> " + val);
                            }
                        } else if (op < 9) {
                            // 10% remove
                            map.remove("key" + key);
                        } else {
                            // 10% putexp with very short TTL to stress eviction
                            long expiry = System.currentTimeMillis() + 20;
                            map.putexp("key" + key, "val" + key, 20);
                            ttlManager.schedule("key" + key, expiry);
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(60, TimeUnit.SECONDS);
        pool.shutdown();

        // let TTL cleanup finish
        Thread.sleep(TTL_MS * 3);

        System.out.println("--- Results ---");
        System.out.println("Completed: " + completed);
        System.out.println("Exceptions: " + errors.get());
        System.out.println("Corrupt reads: " + corruptReads.get());
        System.out.println("Final node_ct: " + map.getNodeCt());

        if (!completed) {
            System.out.println("DEADLOCK — did not finish within 60 seconds");
        } else if (errors.get() > 0 || corruptReads.get() > 0) {
            System.out.println("FAILED");
        } else if (map.getNodeCt() < 0) {
            System.out.println("FAILED — negative node_ct");
        } else {
            System.out.println("PASSED");
        }
    }
}