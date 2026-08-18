package whelk.rest.api;

import io.prometheus.metrics.core.datapoints.Timer;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.core.metrics.Summary;

public class RestMetrics {
    protected static final Counter requests = Counter.builder()
        .name("api_requests_total").help("Total requests to API.")
        .labelNames("method").register();

    protected static final Counter failedRequests = Counter.builder()
        .name("api_failed_requests_total").help("Total failed requests to API.")
        .labelNames("method", "status").register();

    protected static final Gauge ongoingRequests = Gauge.builder()
        .name("api_ongoing_requests").help("Total ongoing API requests.")
        .labelNames("method").register();

    protected static final Summary requestsLatency = Summary.builder()
        .name("api_requests_latency_seconds")
        .help("API request latency in seconds.")
        .labelNames("method")
        .quantile(0.5f, 0.05f)
        .quantile(0.95f, 0.01f)
        .quantile(0.99f, 0.001f)
        .register();

    protected static final Histogram requestsLatencyHistogram = Histogram.builder()
            .name("api_requests_latency_seconds_histogram").help("API request latency in seconds.")
            .labelNames("method")
            .register();

    public Measurement measure(String metricLabel) {
        return new Measurement(metricLabel);
    }

    public static class Measurement {
        String metricLabel;
        Timer latencyTimer;
        Timer latencyHistogramTimer;

        Measurement(String metricLabel) {
            this.metricLabel = metricLabel;
            requests.labelValues(metricLabel).inc();
            ongoingRequests.labelValues(metricLabel).inc();
            latencyTimer = requestsLatency.labelValues(metricLabel).startTimer();
            latencyHistogramTimer = requestsLatencyHistogram.labelValues(metricLabel).startTimer();
        }

        public void complete() {
            ongoingRequests.labelValues(metricLabel).dec();
            latencyTimer.close();
            latencyHistogramTimer.close();
        }
    }
}
