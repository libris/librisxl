package whelk.util;

import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import io.prometheus.client.guava.cache.CacheMetricsCollector;

public class Metrics {
    public static final CacheMetricsCollector cacheMetrics = new CacheMetricsCollector().register();

    public static final Summary clientTimer = Summary.build()
            .labelNames("target", "method")
            .quantile(0.5, 0.05)
            .quantile(0.95, 0.01)
            .quantile(0.99, 0.001)
            .name("client_requests_latency_seconds")
            .help("External request latency in seconds.")
            .register();

    public static final Counter clientCounter = Counter.build()
            .labelNames("target", "method", "status")
            .name("client_call_status")
            .help("External response status.")
            .register();
}
