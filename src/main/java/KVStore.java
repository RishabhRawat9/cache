import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KVStore {

    private static Jmap<String, String> map = new Jmap<>(0.5f) ;
    private static PrintWriter logWriter;
    private static BufferedReader logReader;

    // right now every put, del goes to the log but log can bloat so need to compact
    // it by frequently checking if dup enries so invalid entries are there or not.

    public static void main(String[] args) throws InterruptedException {
        // now before doing this we gotta build the kvStore from the log file;
        // and store all the updates to the log file;

        // in the log file writing as base64 encoding but in memory they are raw bytes
        // only;
//        fillStore();
//        multithreadingTest();

        stressTest();
    }
    public static void stressTest() throws InterruptedException {
        int writers = 10;
        int readers = 40;
        int operationsPerThread = 1000;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(writers + readers);
        ExecutorService executor = Executors.newFixedThreadPool(writers + readers);


        for (int i = 0; i < writers; i++) {
            int threadId = i;
            executor.submit(() -> {
                try {
                    startGate.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        map.put("key-" + threadId + "-" + j, "value-" + j);
                    }
                } catch (InterruptedException ignored) {
                } finally {
                    endGate.countDown();
                }
            });
        }

        for (int i = 0; i < readers; i++) {
            executor.submit(() -> {
                try {
                    startGate.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        // Randomly read keys that might or might not exist yet
                        map.get("key-" + (j % writers) + "-" + j);
                    }
                } catch (InterruptedException ignored) {
                } finally {
                    endGate.countDown();
                }
            });
        }

        long startTime = System.currentTimeMillis();
        startGate.countDown();
        endGate.await();
        long endTime = System.currentTimeMillis();
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        System.out.println("Test duration: " + (endTime - startTime) + "ms");
        System.out.println("Expected nodes: " + (writers * operationsPerThread));
        System.out.println("Actual node_ct: " + Jmap.node_ct.get());

    }

}
