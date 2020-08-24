package whelk.util;

import io.prometheus.client.guava.cache.CacheMetricsCollector;

public class Metrics {
    public static final CacheMetricsCollector cacheMetrics = new CacheMetricsCollector().register();
}
