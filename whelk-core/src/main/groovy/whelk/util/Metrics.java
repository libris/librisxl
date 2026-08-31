package whelk.util;

import io.prometheus.client.guava.cache.CacheMetricsCollector;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Summary;
import io.prometheus.metrics.simpleclient.bridge.SimpleclientCollector;

public class Metrics {
    static {
        SimpleclientCollector.builder().register();
    }

    public static final CacheMetricsCollector cacheMetrics = new CacheMetricsCollector().register();

    public static final Summary clientTimer = Summary.builder()
            .labelNames("target", "method")
            .quantile(0.5, 0.05)
            .quantile(0.95, 0.01)
            .quantile(0.99, 0.001)
            .name("client_requests_latency_seconds")
            .help("External request latency in seconds.")
            .register();

    public static final Counter clientCounter = Counter.builder()
            .labelNames("target", "method", "status")
            .name("client_call_status")
            .help("External response status.")
            .register();
}
