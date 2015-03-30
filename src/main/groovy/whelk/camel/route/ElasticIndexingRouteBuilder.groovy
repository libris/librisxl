package whelk.camel.route

import groovy.util.logging.Slf4j as Log

import whelk.*
import whelk.camel.*

import org.apache.camel.*
import org.apache.camel.component.elasticsearch.aggregation.BulkRequestAggregationStrategy

class ElasticIndexingRouteBuilder extends WhelkRouteBuilderPlugin {

    String messageQueue, bulkMessageQueue, removeQueue
    int elasticBatchSize = 2000

    Processor reindexProcessor, elasticTypeRouteProcessor

    ElasticIndexingRouteBuilder(String ident = null, Map settings) {
        id = ident
        batchTimeout = settings.get("batchTimeout", batchTimeout)

        elasticBatchSize = settings.get("elasticBatchSize", elasticBatchSize)
        messageQueue = settings.get("indexMessageQueue")
        bulkMessageQueue = settings.get("bulkIndexMessageQueue")
        removeQueue = messageQueue
    }

    void bootstrap() {
        elasticTypeRouteProcessor = getElasticProcessor()
        reindexProcessor = getPlugin("camel_reindex_processor")

        assert elasticTypeRouteProcessor
    }

    void configure() {

        BulkRequestAggregationStrategy aggStrat = new BulkRequestAggregationStrategy()

        if (reindexProcessor) {
            from(messageQueue) // Also removeQueue (configured to same)
                .multicast().to("seda:q1", "seda:q2")

            from("seda:q1")
                    .process(elasticTypeRouteProcessor)
                    .routingSlip(header("elasticDestination"))
            from("seda:q2")
                    .process(reindexProcessor)
                    .end()
        } else {
            from(messageQueue)
                .process(elasticTypeRouteProcessor)
                .routingSlip(header("elasticDestination"))
        }

        from(bulkMessageQueue)
                .process(elasticTypeRouteProcessor)
                .aggregate(header("document:dataset"), aggStrat).completionSize(elasticBatchSize).completionTimeout(batchTimeout)
                .routingSlip(header("elasticDestination"))
    }

    Processor getElasticProcessor() {
        Processor ep = getPlugin("elasticprocessor")
        if (!ep) {
            assert whelk
            ep = ElasticRouteProcessor.getInstance()
            ep.shapeComputer = whelk.index
            ep.bootstrap()
        }
        return ep
    }
}


