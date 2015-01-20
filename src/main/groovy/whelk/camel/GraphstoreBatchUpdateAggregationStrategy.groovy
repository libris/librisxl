package whelk.camel

import groovy.util.logging.Slf4j as Log

import whelk.plugin.*

import org.apache.camel.*
import org.apache.camel.processor.aggregate.*

@Log
class GraphstoreBatchUpdateAggregationStrategy extends BasicPlugin implements AggregationStrategy {

    JsonLdToTurtle serializer
    File debugOutputFile = null

    GraphstoreBatchUpdateAggregationStrategy(Map config) {
        def loader = getClass().classLoader
        def contextSrc = loader.getResourceAsStream(config.contextPath).withStream {
            mapper.readValue(it, Map)
        }
        def context = JsonLdToTurtle.parseContext(contextSrc)
        serializer = new JsonLdToTurtle(context, new ByteArrayOutputStream(), config.base)
        if (config.bnodeSkolemBase) {
            serializer.bnodeSkolemBase = config.bnodeSkolemBase
        }
        if (log.isDebugEnabled()) {
            debugOutputFile = new File("aggregator_data_tap.rq")
        }
    }

    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        String identifier = newExchange.getIn().getHeader("document:identifier")
        def bos = new ByteArrayOutputStream()
        serializer.writer = new OutputStreamWriter(bos, "UTF-8")
        if (oldExchange == null) {
            // First message in aggregate
            serializer.prelude() // prefixes and base
            serializer.uniqueBNodeSuffix = "-${System.nanoTime()}"

            // Set contenttype header
            newExchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/sparql-update")

            def obj = newExchange.getIn().getBody(Map.class)

            serializer.writeln "CLEAR GRAPH <$identifier> ;"
            serializer.writeln "INSERT DATA { GRAPH <$identifier> {"
            serializer.flush()
            serializer.objectToTurtle(obj)
            serializer.writeln "} } ;"
            serializer.flush()

            newExchange.getIn().setBody(bos.toByteArray())

            if (debugOutputFile) {
                debugOutputFile << newExchange.getIn().getBody()
            }

            return newExchange
        } else {
            // Append to existing message
            def obj = newExchange.getIn().getBody(Map.class)

            StringBuilder update = new StringBuilder(oldExchange.getIn().getBody(String.class))

            serializer.uniqueBNodeSuffix = "-${System.nanoTime()}"

            serializer.writeln "CLEAR GRAPH <$identifier> ;"
            serializer.writeln "INSERT DATA { GRAPH <$identifier> {"
            serializer.flush()
            serializer.objectToTurtle(obj)
            serializer.writeln "} } ;"
            serializer.flush()

            update.append(bos.toString("UTF-8"))

            oldExchange.getIn().setBody(update.toString().getBytes("UTF-8"))

            if (debugOutputFile) {
                debugOutputFile << oldExchange.getIn().getBody()
            }

            return oldExchange
        }
    }
}
