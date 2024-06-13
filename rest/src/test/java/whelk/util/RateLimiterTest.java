package whelk.util;

import org.junit.Assert;
import org.junit.Test;


public class RateLimiterTest {
    
    @Test
    public void test() {
        int maxRate = 10;
        var limiter = new RateLimiter(maxRate);
        String aKey = "abc";
        long time = 0;
        for (int i = 0 ; i < maxRate - 1 ; i ++) {
            Assert.assertTrue(limiter.isOk(aKey, time));
        }
        Assert.assertFalse(limiter.isOk(aKey, time));
        Assert.assertTrue(limiter.isOk("anotherKey", time));
    }

    @Test
    public void testRecover() {
        int maxRate = 10;
        var limiter = new RateLimiter(maxRate);
        String aKey = "abc";
        long time = 0;
        for (int i = 0 ; i < maxRate - 1 ; i ++) {
            Assert.assertTrue(limiter.isOk(aKey, time));
        }
        Assert.assertFalse(limiter.isOk(aKey, time));

        Assert.assertTrue(limiter.isOk(aKey, time + 101));
        Assert.assertFalse(limiter.isOk(aKey, time + 101));
    }

    @Test
    public void testContinuous() {
        int maxRate = 110;
        var limiter = new RateLimiter(maxRate);
        String aKey = "abc";
        long time = 0;
        
        for (int i = 0 ; i < 10_000 ; i ++) {
            for (int j = 0 ; j < maxRate / 10 ; j ++) {
                for (int k = 0 ; k <  10 ; k ++) {
                    Assert.assertTrue(limiter.isOk(aKey, time));
                }
                time += 100;
            }
        }
    }
}
