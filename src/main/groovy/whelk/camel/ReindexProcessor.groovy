package whelk.camel

import groovy.util.logging.Slf4j as Log
import org.apache.camel.Exchange
import org.apache.camel.Message
import org.apache.camel.Processor
import org.apache.camel.ProducerTemplate
import org.apache.camel.impl.DefaultExchange
import org.apache.camel.impl.DefaultMessage
import whelk.Document
import whelk.Whelk
import whelk.plugin.BasicPlugin
import whelk.result.JsonLdSearchResult

@Log
class ReindexProcessor extends BasicPlugin implements Processor {

    ProducerTemplate template = null
    def fields
    def identifier
    def size = 2

    def lastId

    ReindexProcessor(Map settings) {

        fields =  (settings ? settings.get("fields") : [])
        identifier = (settings ? settings.get("identifier") : "@id")
        log.debug("ReindexProcessor with field: ${fields} for identifier ${identifier}")
    }

    @Override
    void process(Exchange exchange) throws Exception {

        Message message = exchange.getIn()
        def doc = message.getBody(Map.class) as ConfigObject

        if (doc != null) { // <!-- 1
            def props = doc.toProperties()
            def incommingIdentifier = props[identifier]

            if (lastId != incommingIdentifier){
                lastId = incommingIdentifier

                def start = 0
                JsonLdSearchResult jsonResult = doQuery(start, incommingIdentifier)
                def hits = jsonResult.getNumberOfHits()
                for (start; start < hits;) {
                    start += size

                    if(!template)
                        template = exchange.getContext().createProducerTemplate()

                    Map result = jsonResult.toMap(null, [])
                    List items = result.get("items")
                    for (Map item in items) {
                        log.debug("identifiers to add to cue ${item.get("@id")}")
                        Document document = this.whelk.get(item.get("@id"))

                        Exchange ex = new DefaultExchange(exchange.getContext())
                        Message msg = new DefaultMessage()
                        msg.setHeader("document:metaentry", document.entry.inspect())
                        msg.setHeader("document:identifier", document.identifier)
                        msg.setHeader("document:dataset", document.dataset)
                        msg.setHeader("document:metaentry", document.metadataAsJson)
                        msg.setHeader("whelk:operation", Whelk.BULK_ADD_OPERATION)
                        msg.setBody(document.dataAsMap, Map)

                        ex.setIn(msg)
                        template.send(props.get("MQ_BULK_INDEX"), ex)
                    }

                    if (start < hits && size < hits ) {
                        jsonResult = doQuery(start, incommingIdentifier)
                    }
                }
            }
        }//<!-- 1
    }

    private JsonLdSearchResult doQuery(start, incommingIdentifier) {
        def query = "{\"from\": \"${start}\",\"size\": \"${size}\",\"query\": {\"multi_match\":{\"query\":\"${incommingIdentifier}\",\"fields\": [${fields.collect { "\"$it\"" }.join(', ')}] }}}"
        log.debug("Running query [ ${query} ]")
        JsonLdSearchResult jsonResult = this.whelk.index.query(query, 0, 0, "libris", ["auth", "bib", "hold"] as String[])
        return jsonResult
    }
}
