package whelk.camel

import groovy.util.logging.Slf4j as Log
import org.apache.camel.Exchange
import org.apache.camel.Message
import org.apache.camel.Processor
import whelk.Document
import whelk.plugin.BasicPlugin
import whelk.result.JsonLdSearchResult

//TODO Pagination

@Log
class ReindexProcessor extends BasicPlugin implements Processor {

    def fields
    def identifier

    ReindexProcessor(Map settings) {

        fields =  (settings ? settings.get("fields") : [])
        identifier = (settings ? settings.get("identifier") : "@id")
        log.debug("ReindexProcessor with field: ${fields} for identifier ${identifier}")
    }

    @Override
    void process(Exchange exchange) throws Exception {
        List documents = []
        Message message = exchange.getIn()
        def doc = message.getBody(Map.class) as ConfigObject
        def props = doc.toProperties()
        def incommingIdentifier = props[identifier]

        def query = "{\"query\": {\"multi_match\":{\"query\":\"${incommingIdentifier}\",\"fields\": [${fields.collect{ "\"$it\"" }.join(', ')}] }}}" //TODO
        log.debug("Running query [ ${query} ]")
        JsonLdSearchResult jsonResult = this.whelk.index.query(query,0, 0, "libris", ["auth", "bib", "hold"] as String[])

        jsonResult.iterator()

        /* Map result = jsonResult.toMap(null, [])
         List items = result.get("items")
         for (Map item in items) {
             log.debug("identifiers to add to cue ${item.get("@id")}")
             Document document = this.whelk.get(item.get("@id"))
             documents.add(document)

             System.out.println("###### id ${document.identifier}")
         }
         log.debug("Adding documents to cue")*/
        //this.whelk.notifyCamel(documents)
        /*
            0 - 1
            1 - 2
            2 - 3
            3 - 4
            4 - 5
            5 - 6
        */
        /*if (end < jsonResult.getNumberOfHits()) {
            start++
            end++
            process(exchange);
        }*/
    }
}