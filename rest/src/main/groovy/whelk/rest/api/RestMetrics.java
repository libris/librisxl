package whelk.rest.api

import groovy.transform.CompileStatic

import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import io.prometheus.client.Summary

@CompileStatic
class RestMetrics {
    protected static final Counter requests = Counter.build()
        .name("api_requests_total").help("Total requests to API.")
        .labelNames("method").register()

    protected static final Counter failedRequests = Counter.build()
        .name("api_failed_requests_total").help("Total failed requests to API.")
        .labelNames("method", "status").register()

    protected static final Gauge ongoingRequests = Gauge.build()
        .name("api_ongoing_requests_total").help("Total ongoing API requests.")
        .labelNames("method").register()

    protected static final Summary requestsLatency = Summary.build()
        .name("api_requests_latency_seconds")
        .help("API request latency in seconds.")
        .labelNames("method")
        .quantile(0.5f, 0.05f)
        .quantile(0.95f, 0.01f)
        .quantile(0.99f, 0.001f)
        .register()

    protected static final Histogram requestsLatencyHistogram = Histogram.build()
            .name("api_requests_latency_seconds_histogram").help("API request latency in seconds.")
            .labelNames("method")
            .register()

    Measurement measure(String metricLabel) {
        return new Measurement(metricLabel)
    }

    class Measurement {
        String metricLabel
        Summary.Timer requestTimer
        Histogram.Timer requestTimer2

        Measurement(String metricLabel) {
            this.metricLabel = metricLabel
            RestMetrics.this.requests.labels(metricLabel).inc()
            RestMetrics.this.ongoingRequests.labels(metricLabel).inc()
            requestTimer = requestsLatency.labels(metricLabel).startTimer()
            requestTimer2 = requestsLatencyHistogram.labels(metricLabel).startTimer()
        }
        void complete() {
            RestMetrics.this.ongoingRequests.labels(metricLabel).dec()
            requestTimer.observeDuration()
            requestTimer2.observeDuration()
        }
    }
}
