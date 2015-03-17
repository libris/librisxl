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
        String newIndexId = newExchange.getIn().getHeader("indexId")
        String newParentId = newExchange.getIn().getHeader("parentId")
        ArrayList<Object> list = null
        ArrayList<String> indexList = null
        ArrayList<String> parentList = null

        if (oldExchange == null) {
            list = new ArrayList<Object>()
            indexList = new ArrayList<String>()
            parentList = new ArrayList<String>()

            list.add(newBody)
            indexList.add(newIndexId)
            parentList.add(newParentId)

            newExchange.getIn().setBody(list)
            newExchange.getIn().setHeader("indexId", indexList)
            newExchange.getIn().setHeader("parentId", parentList)


            return newExchange
        } else {
            list = oldExchange.getIn().getBody(ArrayList.class)
            indexList = oldExchange.getIn().getHeader("indexId")
            parentList = oldExchange.getIn().getHeader("parentId")

            list.add(newBody)
            indexList.add(newIndexId)
            parentList.add(newParentId)

            return oldExchange
        }
    }
}


