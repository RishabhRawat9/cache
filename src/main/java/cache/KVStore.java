package cache;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KVStore {

    // private static ConcurrentHashMap<String, String> map = new
    // ConcurrentHashMap<>(16, 0.75f);
    private static Jmap<String, String> map = new Jmap<>(0.75f);
    private static TTLManager<String, String> ttlManager;
    private static PrintWriter logWriter;
    private static final String LOG_FILE = "src/main/logs/logs.txt";

    public static void main(String[] args) throws InterruptedException {
        // ttlManager = new TTLManager<String, String>(map);
        // initializeLog();
        // loadLog();
        // interactiveMode();
        stressTest();
    }

    private static void initializeLog() {
        try {
            File logDir = new File("src/main/logs");
            if (!logDir.exists()) {
                logDir.mkdirs();
            }
            logWriter = new PrintWriter(new BufferedWriter(new FileWriter(LOG_FILE, true)));
        } catch (IOException e) {
            System.err.println("Could not initialize log writer: " + e.getMessage());
        }
    }

    private static void loadLog() {
        File file = new File(LOG_FILE);
        if (!file.exists())
            return;

        System.out.println("Loading data from log file...");
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+");
                if (parts.length >= 2) {
                    String op = parts[0];
                    String key = parts[1];
                    if (op.equalsIgnoreCase("put") && parts.length == 3) {
                        map.put(key, parts[2]);
                        count++;
                    } else if (op.equalsIgnoreCase("del") || op.equalsIgnoreCase("remove")) {
                        try {
                            map.remove(key);
                            count++;
                        } catch (Exception ignored) {
                        }
                    }
                }
            }
            System.out.println("Restored " + count + " operations from log file.");
        } catch (IOException e) {
            System.err.println("Could not load log file: " + e.getMessage());
        }
    }

    private static void log(String op, String key, String value) {
        if (logWriter != null) {
            if (value != null) {
                logWriter.println(op + " " + key + " " + value);
            } else {
                logWriter.println(op + " " + key);
            }
            logWriter.flush();
        }
    }

    // to deal with multi word keys/values
    private static String[] parseInput(String input) {
        ArrayList<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);

            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (Character.isWhitespace(c) && !inQuotes) {
                if (current.length() > 0) {
                    parts.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            parts.add(current.toString());
        }

        return parts.toArray(new String[0]);
    }

    public static void interactiveMode() {
        Scanner scanner = new Scanner(System.in);
        try {
            System.out.println("Welcome to Jmap CLI. Type 'help' for commands.");
            while (true) {
                System.out.print("> ");
                String input = scanner.nextLine().trim();
                if (input.isEmpty())
                    continue;

                String[] parts = parseInput(input);

                String command = parts[0].toLowerCase();

                try {
                    switch (command) {
                        case "put":
                            if (parts.length == 3) {
                                map.put(parts[1], parts[2]);
                                log("put", parts[1], parts[2]);
                                System.out.println("OK");
                            } else {
                                System.out.println("Usage: put <key> <value>");
                            }
                            break;
                        case "putexp":
                            if (parts.length == 4) {
                                long ttlSeconds = Long.parseLong(parts[3]);
                                long expiryTime = System.currentTimeMillis() + (ttlSeconds);
                                map.putexp(parts[1], parts[2], expiryTime);
                                ttlManager.schedule(parts[1], expiryTime);//now both the manager and queue take the same expiry value ;
                                log("putexp", parts[1], parts[2] + " " + ttlSeconds);
                                System.out.println("OK");
                            } else {
                                System.out.println("Usage: putexp <key> <value> <ttl_seconds>");
                            }
                            break;
                        case "get":
                            if (parts.length == 2) {
                                String value = map.get(parts[1]);
                                System.out.println(value != null ? value : "(nil)");
                            } else {
                                System.out.println("Usage: get <key>");
                            }
                            break;
                        case "del":
                        case "remove":
                            if (parts.length == 2) {
                                map.remove(parts[1]);
                                log("del", parts[1], null);
                                System.out.println("OK");
                            } else {
                                System.out.println("Usage: del <key>");
                            }
                            break;
                        case "show":
                        case "display":
                            System.out.println(map.toString());
                            break;
                        case "count":
                        case "size":
                            System.out.println("Node count: " + map.getNodeCt());
                            break;
                        case "help":
                            System.out.println(
                                    "Commands: putexp <k> <v> <tll> ,put <k> <v>, get <k>, del <k>, show, count, exit");
                            break;
                        case "exit":
                        case "quit":
                            if (logWriter != null)
                                logWriter.close();
                            if (ttlManager != null)
                                ttlManager.shutdown();
                            System.out.println("Goodbye!");
                            return;
                        default:
                            System.out.println("Unknown command. Type 'help' for available commands.");
                    }
                } catch (Exception e) {
                    System.out.println("Error: " + e.getMessage());
                }
            }
        } finally {
            scanner.close();
        }
    }

    public static void stressTest() throws InterruptedException {
        System.out.println("running stress test... in kkvstore");
        int writers = 10;
        int readers = 10;
        int operationsPerThread = 100;
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
        // System.out.println("actual nodes: " + map.size());
        System.out.println("Actual node_ct: " + map.getNodeCt());
    }
}
