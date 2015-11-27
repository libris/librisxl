package whelk.integration.filter

/**
 * Created by ina on 15-11-25.
 */

import org.apache.camel.Exchange
import org.apache.log4j.Logger

class FilterMessagesForAPIX {

    Logger logger = Logger.getLogger(FilterMessagesForAPIX.class.getName());

    boolean isAddOrDelete(Exchange exchange) {
        Map body = exchange.getIn().getBody()
        String operation = body.info.operation
        logger.debug("Checking incoming apix operation: " + operation)
        return (operation == 'ADD' || operation == 'DELETE')
    }

    boolean isRelevantDataset(Exchange exchange) {
        Map body = exchange.getIn().getBody()
        String id = body.info.id
        logger.debug("Checking incoming id: " + id)
        return id ==~ /\/(auth|bib|hold)\/\d+/
    }
}
