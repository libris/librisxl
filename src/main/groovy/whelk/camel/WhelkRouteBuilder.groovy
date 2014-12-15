package whelk.camel

import groovy.util.logging.Slf4j as Log

import whelk.Whelk
import whelk.plugin.*

import org.apache.camel.*
import org.apache.camel.impl.*
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.model.dataformat.JsonLibrary
import org.apache.camel.processor.aggregate.*

import org.apache.camel.component.http4.*

import org.codehaus.jackson.map.ObjectMapper

class WhelkRouteBuilder extends RouteBuilder implements WhelkAware {

    // Properties copied from BasicPlugin
    String id = null
    private List<Plugin> plugins = new ArrayList<Plugin>();
    Map global

    Whelk whelk

    int elasticBatchSize = 2000
    int graphstoreBatchSize = 1000
    long batchTimeout = 5000
    int parallelProcesses = 20
    String indexMessageQueue
    String bulkIndexMessageQueue
    String graphstoreMessageQueue
    String apixUri = null

    WhelkRouteBuilder(Map settings) {
        elasticBatchSize = settings.get("elasticBatchSize", elasticBatchSize)
        graphstoreBatchSize = settings.get("graphstoreBatchSize", graphstoreBatchSize)
        batchTimeout = settings.get("batchTimeout", batchTimeout)
        indexMessageQueue = settings.get("indexMessageQueue")
        bulkIndexMessageQueue = settings.get("bulkIndexMessageQueue")
        graphstoreMessageQueue = settings.get("graphstoreMessageQueue")
        apixUri = settings.get("apixUri")
        if (apixUri) {
            apixUri = apixUri.replace("http://", "http4:")
            apixUri = apixUri.replace("https://", "https4:")
            def properties = new java.util.Properties()
            properties.load(this.getClass().getClassLoader().getResourceAsStream("api.properties"))
            apixUri = apixUri + "?" +
                "authUsername=" + properties.getProperty("apixUsername") + "&" +
                "authPassword=" + properties.getProperty("apixPassword") + "&" +
                "authenticationPreemptive=true" + "&" +
                "httpClient.redirectsEnabled=false"
        }
    }

    void configure() {
        Processor formatConverterProcessor = getPlugin("elastic_camel_processor")
        AggregationStrategy graphstoreAggregationStrategy = getPlugin("graphstore_aggregator")
        APIXProcessor apixProcessor = getPlugin("apixprocessor")

        String primaryStorageId = whelk.storage.id

        def eligbleMQs = []
        def eligbleBulkMQs = []
        if (whelk.index) {
            eligbleMQs.add(indexMessageQueue)
            eligbleBulkMQs.add(bulkIndexMessageQueue)
        }
        if (whelk.graphStore) {
            eligbleMQs.add(graphstoreMessageQueue)
            eligbleBulkMQs.add(graphstoreMessageQueue)
        }
        if (apixUri) {
            eligbleMQs.add("activemq:apix.queue")
            //eligbleBulkMQs.add("activemq:apix.queue")
        }

        onException(HttpOperationFailedException.class)
            .handled(true)
            .bean(new HttpFailedBean(), "handle")
            .choice()
                .when(header("retry"))
                    .to("activemq:"+whelk.id+".retries")
                .otherwise()
                    .end()

        from("activemq:"+whelk.id+".retries").delay(10000).routingSlip(header("next"))

        if (eligbleMQs.size() > 0) {
            from("direct:"+whelk.id).process(formatConverterProcessor).multicast().parallelProcessing().to(eligbleMQs as String[])
        }
        if (eligbleBulkMQs.size() > 0) {
            from("direct:bulk_"+whelk.id).process(formatConverterProcessor).multicast().parallelProcessing().to(eligbleBulkMQs as String[])
        }

        if (whelk.index) {
            from(indexMessageQueue)
                .process(new ElasticTypeRouteProcessor(whelk.index))
                .routingSlip(header("elasticDestination"))

            from(bulkIndexMessageQueue)
                .process(new ElasticTypeRouteProcessor(whelk.index))
                .aggregate(header("entry:dataset"), new ArrayListAggregationStrategy()).completionSize(elasticBatchSize).completionTimeout(batchTimeout)
                .routingSlip(header("elasticDestination"))

        }

        // Routes for graphstore
        if (whelk.graphStore) {
            def credentials = global.GRAPHSTORE_UPDATE_AUTH_USER?
                "?authenticationPreemptive=true&authUsername=${global.GRAPHSTORE_UPDATE_AUTH_USER}&authPassword=${global.GRAPHSTORE_UPDATE_AUTH_PASS}" : ""

            def camelStep = from(graphstoreMessageQueue)
                .filter("groovy", "request.getHeader('whelk:operation') != 'DELETE' && ['auth','bib'].contains(request.getHeader('entry:dataset'))") // Only save auth and bib
                .aggregate(header("entry:dataset"), graphstoreAggregationStrategy).completionSize(graphstoreBatchSize).completionTimeout(batchTimeout)
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

        if (apixUri) {
            from("activemq:apix.queue")
                .filter("groovy", "['auth','bib','hold'].contains(request.getHeader('entry:dataset'))") // Only save auth hold and bib
                .process(apixProcessor)
                .to(apixUri)
                .process(new APIXResponseProcessor())
        }
    }

    // Plugin methods
    @Override
    public void init(String initString) {}
    @Override
    public void addPlugin(Plugin p) {
        plugins.add(p);
    }
    public List<Plugin> getPlugins() { plugins }

    public Plugin getPlugin(String pluginId) {
        return plugins.find { it.id == pluginId }
    }
}

@Log
class HttpFailedBean {

    void handle(Exchange exchange, Exception e) {
        log.info("Handling non 2xx http response (${e.statusCode}).")
        Message message = exchange.getIn()
        // TODO: More fine grained error handling
        if (e.statusCode < 400) {
            log.info("received status ${e.statusCode} from http, setting handled=true")
            message.setHeader("handled", true)
        } else if (e.statusCode == 404) {
            message.setHeader("handled", true)
            log.warn("Tried to ${message.getHeader('whelk:operation')} ${message.getHeader('entry:identifier')} but got a 404. Guess it's not there ...")
        } else {
            log.info("Failed to deliver to ${e.uri} with status ${e.statusCode}. Sending message to retry queue.")
            message.setHeader("handled", false)
            message.setHeader("retry", true)
            message.setHeader("next", message.getHeader("JMSDestination").toString().replace("queue://", "activemq:"))
        }
    }
}

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
        String identifier = newExchange.getIn().getHeader("entry:identifier")
        def bos = new ByteArrayOutputStream()
        serializer.writer = new OutputStreamWriter(bos, "UTF-8")
        if (oldExchange == null) {
            // First message in aggregate
            serializer.prelude() // prefixes and base
            serializer.uniqueBNodeSuffix = "-${System.nanoTime()}"

            // Set contenttype header
            newExchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/sparql-update")

            def obj = mapper.readValue(new String(newExchange.getIn().getBody(), "UTF-8"), Map)

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
            def obj = mapper.readValue(new String(newExchange.getIn().getBody(), "UTF-8"), Map)

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

@Log
class ArrayListAggregationStrategy implements AggregationStrategy {
    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        log.debug("Called aggregator for message in dataset: ${newExchange.in.getHeader("entry:dataset")}")
        Object newBody = newExchange.getIn().getBody()
        ArrayList<Object> list = null
        if (oldExchange == null) {
            list = new ArrayList<Object>()
            list.add(newBody)
            newExchange.getIn().setBody(list)
            return newExchange
        } else {
            list = oldExchange.getIn().getBody(ArrayList.class)
            list.add(newBody)
            return oldExchange
        }
    }
}

@Log
class ComputeDestinationSlip {
    public String compute(String body) {
        log.info("body is $body")
        return "mock:result"
    }
}
