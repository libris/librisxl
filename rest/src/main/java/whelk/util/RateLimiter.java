package whelk.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class RateLimiter {
    private final int UPDATE_PERIOD_MS = 100;
    
    // Make the calculations work with really low max rates
    private final long SCALE_FACTOR = 1000;
    
    private ConcurrentMap<String, AtomicLong> buckets = new ConcurrentHashMap<>();
    private AtomicLong lastUpdate = new AtomicLong(-1);
    private int maxRateHz;
    
    public RateLimiter(int maxRateHz) {
        this.maxRateHz = maxRateHz;
    }
    
    public boolean isOk(String key) {
        return isOk(key, System.currentTimeMillis());
    }
    
    public boolean isOk(String key, long currentTimeMillis) {
        maybeDrainBuckets(currentTimeMillis);
        
        var bucket = buckets.computeIfAbsent(key, k -> new AtomicLong());
        long rate = bucket.accumulateAndGet(SCALE_FACTOR, (current, update) -> Math.min(current + update, maxRateHz * SCALE_FACTOR));
        
        return rate < maxRateHz * SCALE_FACTOR;
    }
    
    private void maybeDrainBuckets(long currentTimeMillis) {
        long previousUpdate = lastUpdate.get();
        boolean isTimeToUpdate = currentTimeMillis == lastUpdate.accumulateAndGet(currentTimeMillis, (last, now) ->
                now - last >= UPDATE_PERIOD_MS || last == -1 ? now : last
        );
        if (!isTimeToUpdate) {
            return;
        }
        
        long deltaMs = currentTimeMillis - previousUpdate;
        long drainAmount = (long) ((deltaMs / 1000.0) * maxRateHz * SCALE_FACTOR);
        buckets.forEach((key, bucket) -> {
            if (bucket.accumulateAndGet(drainAmount, (current, update) -> Math.max(current - update, 0)) == 0) {
                // we might miss some increments here, that's ok
                buckets.remove(key);
            }
        });
    }
}
