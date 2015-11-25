package whelk.integration.process

import org.apache.camel.Exchange
import org.apache.camel.Processor
import org.apache.camel.Message

import org.apache.log4j.Logger

class APIXTestProcessor implements org.apache.camel.Processor {

    Logger logger = Logger.getLogger(APIXTestProcessor.class.getName())

    @Override
    public void process(Exchange exchange) throws Exception {
        Map messageBody = exchange.getIn().getBody()

        String operation = messageBody["info"]["operation"]
        String id = messageBody["info"]["id"]
        Map documentData = messageBody["documentData"]
        Map metaData = messageBody["metaData"]

        logger.info("Operation: " + operation)
        logger.info("Id: " + id)
        logger.info("Doc metadata: " + metaData.toString())

    }
}
