package whelk.integration.filter

/**
 * Created by ina on 15-11-25.
 */

import org.apache.camel.Exchange

class FilterMessagesForAPIX {

    boolean isAddOrDelete(Exchange exchange) {
        Map body = exchange.getIn().getBody()
        String operation = body.info.operation
        return (operation == 'ADD' || operation == 'DELETE')
    }

    boolean isRelevantDataset(Exchange exchange) {
        Map body = exchange.getInt().getBody()
        String id = body.info.id
        return id ==~ /\/(auth|bib|hold)\/\d+/
    }
}
