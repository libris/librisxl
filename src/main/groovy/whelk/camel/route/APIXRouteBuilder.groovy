package whelk.camel.route

import groovy.util.logging.Slf4j as Log

import whelk.camel.*

import org.apache.camel.*
import org.apache.camel.component.http4.*

class APIXRouteBuilder extends WhelkRouteBuilderPlugin {

    String messageQueue, bulkMessageQueue, removeQueue, retriesQueue, apixUri = null

    APIXRouteBuilder(Map settings) {
        properties.load(this.getClass().getClassLoader().getResourceAsStream("whelk.properties"))

        messageQueue = settings.get("apixMessageQueue")
        retriesQueue = settings.get("retriesQueue")
        removeQueue = messageQueue
        apixUri = settings.get("apixUri")
        if (apixUri) {
            apixUri = apixUri.replace("http://", "http4:")
            apixUri = apixUri.replace("https://", "https4:")
            apixUri = apixUri + "?" +
                "authUsername=" + properties.getProperty("apixUsername") + "&" +
                "authPassword=" + properties.getProperty("apixPassword") + "&" +
                "authenticationPreemptive=true" + "&" +
                "httpClient.redirectsEnabled=false"
        }
    }

    void bootstrap() {
    }

    @Override
    void configure() {
        Processor formatConverterProcessor = getPlugin("camel_format_processor")
        APIXProcessor apixProcessor = getPlugin("apixprocessor")
        APIXResponseProcessor apixResponseProcessor = getPlugin("apixresponseprocessor")
        APIXHttpResponseFailedBean apixFailureBean = new APIXHttpResponseFailedBean(apixResponseProcessor)

        onException(HttpOperationFailedException.class)
            .handled(true)
            .bean(apixFailureBean, "handle")
            .choice()
                .when(header("retry"))
                    .to(retriesQueue)
                .otherwise()
                    .end()

        from(retriesQueue).delay(60000).routingSlip(header("next"))


        from(messageQueue)
            .filter("groovy", "['auth','bib','hold'].contains(request.getHeader('document:dataset'))") // Only save auth hold and bib
            .process(apixProcessor)
            .to(apixUri)
            .process(apixResponseProcessor)
    }

}

@Log
class APIXHttpResponseFailedBean {

    APIXResponseProcessor apixResponseProcessor

    APIXHttpResponseFailedBean(APIXResponseProcessor arp) {
        log.info("Instantiating APIXHttpResponseFailedBean.")
        this.apixResponseProcessor = arp
    }

    void handle(Exchange exchange, Exception e) {
        log.info("Handling non 2xx http response (${e.statusCode}).")
        Message message = exchange.getIn()
        if (e.statusCode == 303) {
            log.debug("All is well. Got statuscode ${e.statusCode}. Sending exchange to response processor.")
            exchange.getIn().setHeader("CamelHttpResponseCode", e.statusCode)
            exchange.getIn().setHeader("Location", message.getHeader("CamelHttpPath"))
            log.debug("Handling a 303 from APIX, send it to APIX response processor.")
            apixResponseProcessor.process(exchange)
        } else if (e.statusCode == 404) {
            log.info("received status ${e.statusCode} from http for ${message.getHeader('CamelHttpPath')}, setting handled=true")
            message.setHeader("handled", true)
        } else if (e.statusCode < 500) {
            log.info("Failed to deliver to ${e.uri} with status ${e.statusCode}. Sending message to retry queue.")
            message.setHeader("handled", false)
            message.setHeader("retry", true)
            message.setHeader("next", message.getHeader("JMSDestination").toString().replace("queue://", "activemq:"))
        } else {
            log.error("Unrecoverable error received: ${e.statusCode}", e)
            message.headers.each { key, value ->
                log.info("header: $key = $value")
            }
            log.info("message body: ${message.body}")
        }
    }
}
