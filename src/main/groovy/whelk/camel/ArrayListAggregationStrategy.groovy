package whelk.camel

import groovy.util.logging.Slf4j as Log

import org.apache.camel.*
import org.apache.camel.impl.*
import org.apache.camel.processor.aggregate.*

@Log
class ArrayListAggregationStrategy implements AggregationStrategy {


    private static final instance = new ArrayListAggregationStrategy();

    static ArrayListAggregationStrategy getInstance() {
        return instance
    }

    private ArrayListAggregationStrategy() {}

    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        log.debug("Called aggregator for message in dataset: ${newExchange.in.getHeader("document:dataset")}")
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


