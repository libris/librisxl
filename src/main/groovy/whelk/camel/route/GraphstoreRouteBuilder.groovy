package whelk.camel.route

import groovy.util.logging.Slf4j as Log

import whelk.camel.*

import org.apache.camel.*
import org.apache.camel.impl.*
import org.apache.camel.processor.aggregate.*

import org.apache.camel.component.http4.*

class GraphstoreRouteBuilder extends WhelkRouteBuilderPlugin {

    int graphstoreBatchSize = 1000
    String messageQueue, bulkMessageQueue, removeQueue,retriesQueue

    GraphstoreRouteBuilder(Map settings) {
        batchTimeout = settings.get("batchTimeout", batchTimeout)
        messageQueue = settings.get("graphstoreMessageQueue")
        bulkMessageQueue = settings.get("graphstoreMessageQueue")
        retriesQueue = settings.get("retriesQueue")
        try {
            properties.load(this.getClass().getClassLoader().getResourceAsStream("whelk.properties"))
        } catch (Exception ex) {
            log.warn("Failed to load whelk.properties.")
        }
    }

    void bootstrap() {}

    void configure() {
        AggregationStrategy graphstoreAggregationStrategy = getPlugin("graphstore_aggregator")
        GraphstoreHttpResponseFailedBean graphstoreFailureBean = new GraphstoreHttpResponseFailedBean(messageQueue)

        def credentials = props.GRAPHSTORE_UPDATE_AUTHENTICATION_REQUIRED?
        "?authenticationPreemptive=true&authUsername=${properties.getProperty("graphstoreUpdateAuthUser")}&authPassword=${properties.getProperty("graphstoreUpdateAuthPass")}" : ""

        onException(HttpOperationFailedException.class, org.apache.http.NoHttpResponseException.class, org.apache.http.conn.HttpHostConnectException)
            .bean(graphstoreFailureBean, "handle")
            .handled(true)
            .choice()
                .when(header("retry"))
                    .to(retriesQueue)
                .otherwise()
                    .end()

        from(retriesQueue).delay(header("delay")).to(messageQueue)

        def camelStep = from(messageQueue)
            .filter("groovy", "request.getHeader('whelk:operation') != 'DELETE' && ['auth','bib','hold'].contains(request.getHeader('document:dataset'))")
            .aggregate(header("document:dataset"), graphstoreAggregationStrategy).completionSize(graphstoreBatchSize).completionTimeout(batchTimeout)
            .threads(1,parallelProcesses)

        def postParameter = props.GRAPHSTORE_UPDATE_POST_PARAMETER
        if (postParameter) {
            camelStep.process {
                def msg = it.getIn()
                msg.setHeader(Exchange.CONTENT_TYPE, "application/x-www-form-urlencoded")
                def body = (postParameter + "=" +
                URLEncoder.encode(msg.getBody(String), "UTF-8").replaceAll(/\+/, "%20")).getBytes("UTF-8")
                msg.setBody(body)
            }
        }

        camelStep.to("http4:${props.GRAPHSTORE_UPDATE_URI.substring(7)}" + credentials)
    }
}

@Log
class GraphstoreHttpResponseFailedBean {

    String graphstoreQueue = null

    GraphstoreHttpResponseFailedBean(String gsq) {
        this.graphstoreQueue = gsq
    }

    void handle(Exchange exchange, Exception e) {
        Message message = exchange.getIn()
        if (e instanceof HttpOperationFailedException) {
            if (e.statusCode < 400) {
                log.debug("Received normal response. (${e.statusCode})")
            } else if (e.statusCode > 399 && e.statusCode < 500) {
                log.error("Client error: ${e.statusCode}. Details: ${e.message}")
            } else if (e.statusCode == 503) {
                log.info("Graphstore is currently unavailable. Sending message to retry.")
                message.setHeader("retry", true)
                message.setHeader("delay", 30000)
                message.setHeader("next", graphstoreQueue)
            } else {
                log.error("Failed to sparql-update. Graphstore responded with error code ${e.statusCode}. Details: ${e.message}.")
                if (log.isDebugEnabled()) {
                    message.headers.each { key, value ->
                        log.debug("header: $key = $value")
                    }
                }
                log.trace("message body: ${message.body}")
            }
        } else {
            log.error("Graphstore update failed with ${e.getName().getClass()}: ${e.message}")
            message.setHeader("retry", true)
            message.setHeader("delay", 3600000)
        }
    }
}
