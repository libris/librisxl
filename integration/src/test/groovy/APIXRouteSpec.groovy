package whelk.integration

import spock.lang.Specification

import org.apache.camel.impl.DefaultExchange
import org.apache.camel.impl.DefaultCamelContext
import org.apache.camel.impl.DefaultMessage

import whelk.integration.filter.FilterMessagesForAPIX

class APIXRouteSpec extends Specification {

    def exchange = new DefaultExchange(new DefaultCamelContext())
    def exchangeMessage = new DefaultMessage()
    def filterer = new FilterMessagesForAPIX()

    def "filtering should accept messages with relevant dataset"() {
        given:
        exchangeMessage.setBody([info: [id: "/bib/12345"]])
        exchange.setIn(exchangeMessage)

        when:
        boolean result = filterer.isRelevantDataset(exchange)

        then:
        result == true

    }

    def "filtering should not accept messages with irrelevant dataset"() {
        given:
        exchangeMessage.setBody([info: [id: "/hej/12345"]])
        exchange.setIn(exchangeMessage)

        when:
        boolean result = filterer.isRelevantDataset(exchange)

        then:
        result == false

    }

    def "filtering should accept messages with relevant operation"() {
        given:
        exchangeMessage.setBody([info: [operation: "ADD"]])
        exchange.setIn(exchangeMessage)

        when:
        boolean result = filterer.isAddOrDelete(exchange)

        then:
        result == true

    }

    def "filtering should not accept messages with irrelevant operation"() {
        given:
        exchangeMessage.setBody([info: [operation: "HEJ"]])
        exchange.setIn(exchangeMessage)

        when:
        boolean result = filterer.isAddOrDelete(exchange)

        then:
        result == false

    }
}
