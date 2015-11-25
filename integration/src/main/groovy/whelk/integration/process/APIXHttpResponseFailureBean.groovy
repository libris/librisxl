package whelk.integration.process

import org.apache.log4j.Logger;

import org.apache.camel.Exchange;
import org.apache.camel.Message;

class APIXHttpResponseFailureBean {

    Logger logger = Logger.getLogger(LibrisIntegrationCamelRouteBuilder.class.getName());

    APIXResponseProcessor apixResponseProcessor

    APIXHttpResponseFailureBean(APIXResponseProcessor arp) {
        this.apixResponseProcessor = arp
    }

    void handle(Exchange exchange, Exception e) {

        logger.debug("Handling non 2xx http response (${e.statusCode}).")
        Message message = exchange.getIn()

        if (e.statusCode == 303) {

            logger.debug("All is well. Got statuscode ${e.statusCode}. Sending exchange to response processor.")

            exchange.getIn().setHeader("CamelHttpResponseCode", e.statusCode)
            exchange.getIn().setHeader("Location", message.getHeader("CamelHttpPath"))

            logger.debug("Handling a 303 from APIX, send it to APIX response processor.")

            apixResponseProcessor.process(exchange)

        } else if (e.statusCode == 404) {

            logger.debug("received status ${e.statusCode} from http for ${message.getHeader('CamelHttpPath')}, setting handled=true")

            message.setHeader("handled", true)

        } else if (e.statusCode < 500) {

            logger.debug("Failed to deliver to ${e.uri} with status ${e.statusCode}. Sending message to retry queue.")

            message.setHeader("handled", false)
            message.setHeader("retry", true)
            message.setHeader("next", message.getHeader("JMSDestination").toString().replace("queue://", "activemq:"))

        } else {

            logger.error("Unrecoverable error received: ${e.statusCode}", e)
            message.headers.each { key, value ->
                logger.debug("header: $key = $value")
            }

            logger.debug("message body: ${message.body}")
        }
    }
}
