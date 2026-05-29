package cache;



import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class JmapBenchmark {

    private Jmap<String, String> jmap;
    private ConcurrentHashMap<String, String> chm;

    private static final int KEY_SPACE = 10_000;

    @Setup
    public void setup() {
        jmap = new Jmap<>();
        chm = new ConcurrentHashMap<>();
        for (int i = 0; i < KEY_SPACE; i++) {
            jmap.put("key" + i, "val" + i);
            chm.put("key" + i, "val" + i);
        }
    }

    @Benchmark
    @Threads(1)
    public void jmapSingleThread(Blackhole bh) {
        int key = ThreadLocalRandom.current().nextInt(KEY_SPACE);
        bh.consume(jmap.get("key" + key));
    }

    @Benchmark
    @Threads(1)
    public void chmSingleThread(Blackhole bh) {
        int key = ThreadLocalRandom.current().nextInt(KEY_SPACE);
        bh.consume(chm.get("key" + key));
    }

    @Benchmark
    @Threads(8)
    public void jmap8Threads(Blackhole bh) {
        int key = ThreadLocalRandom.current().nextInt(KEY_SPACE);
        if (ThreadLocalRandom.current().nextInt(10) < 9) {
            bh.consume(jmap.get("key" + key));
        } else {
            jmap.put("key" + key, "val" + key);
        }
    }

    @Benchmark
    @Threads(8)
    public void chm8Threads(Blackhole bh) {
        int key = ThreadLocalRandom.current().nextInt(KEY_SPACE);
        if (ThreadLocalRandom.current().nextInt(10) < 9) {
            bh.consume(chm.get("key" + key));
        } else {
            chm.put("key" + key, "val" + key);
        }
    }

    @Benchmark
    @Threads(16)
    public void jmap16Threads(Blackhole bh) {
        int key = ThreadLocalRandom.current().nextInt(KEY_SPACE);
        if (ThreadLocalRandom.current().nextInt(10) < 9) {
            bh.consume(jmap.get("key" + key));
        } else {
            jmap.put("key" + key, "val" + key);
        }
    }

    @Benchmark
    @Threads(16)
    public void chm16Threads(Blackhole bh) {
        int key = ThreadLocalRandom.current().nextInt(KEY_SPACE);
        if (ThreadLocalRandom.current().nextInt(10) < 9) {
            bh.consume(chm.get("key" + key));
        } else {
            chm.put("key" + key, "val" + key);
        }
    }
}