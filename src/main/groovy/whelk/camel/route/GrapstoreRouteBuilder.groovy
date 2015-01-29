package whelk.camel.route

import whelk.camel.*

import org.apache.camel.*
import org.apache.camel.impl.*
import org.apache.camel.processor.aggregate.*

class GraphstoreRouteBuilder extends WhelkRouteBuilderPlugin {

    int graphstoreBatchSize = 1000
    String messageQueue, bulkMessageQueue, removeQueue

    GraphstoreRouteBuilder(Map settings) {
        batchTimeout = settings.get("batchTimeout", batchTimeout)
        messageQueue = settings.get("graphstoreMessageQueue")
        bulkMessageQueue = settings.get("graphstoreMessageQueue")
        try {
            properties.load(this.getClass().getClassLoader().getResourceAsStream("whelk.properties"))
        } catch (Exception ex) {
            log.warn("Failed to load whelk.properties.")
        }
    }

    void configure() {
        AggregationStrategy graphstoreAggregationStrategy = getPlugin("graphstore_aggregator")
        def credentials = global.GRAPHSTORE_UPDATE_AUTHENTICATION_REQUIRED?
        "?authenticationPreemptive=true&authUsername=${properties.getProperty("graphstoreUpdateAuthUser")}&authPassword=${properties.getProperty("graphstoreUpdateAuthPass")}" : ""
        def camelStep = from(messageQueue)
            .filter("groovy", "request.getHeader('whelk:operation') != 'DELETE' && ['auth','bib','hold'].contains(request.getHeader('document:dataset'))")
            .aggregate(header("document:dataset"), graphstoreAggregationStrategy).completionSize(graphstoreBatchSize).completionTimeout(batchTimeout)
            .threads(1,parallelProcesses)

        def postParameter = global.GRAPHSTORE_UPDATE_POST_PARAMETER
        if (postParameter) {
            camelStep.process {
                def msg = it.getIn()
                msg.setHeader(Exchange.CONTENT_TYPE, "application/x-www-form-urlencoded")
                def body = (postParameter + "=" +
                URLEncoder.encode(msg.getBody(String), "UTF-8").replaceAll(/\+/, "%20")).getBytes("UTF-8")
                msg.setBody(body)
            }
        }

        camelStep.to("http4:${global.GRAPHSTORE_UPDATE_URI.substring(7)}" + credentials)
    }
}

