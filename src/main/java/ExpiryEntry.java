import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class ExpiryEntry implements Delayed {
    private final String key;
    private final long expiryTime; // absolute time in millis

    public ExpiryEntry(String key, long expiryTime) {
        this.key = key;
        this.expiryTime = expiryTime;
    }

    public String getKey() {
        return key;
    }

    public long getExpiryTime() {
        return expiryTime;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long diff = expiryTime - System.currentTimeMillis();
        return unit.convert(diff, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        if (other instanceof ExpiryEntry) {
            return Long.compare(this.expiryTime, ((ExpiryEntry) other).expiryTime);
        }
        return Long.compare(this.getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
    }
}
