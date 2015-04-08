package whelk.camel

import groovy.util.logging.Slf4j as Log

import org.apache.camel.Exchange
import org.apache.camel.Processor
import org.apache.http.client.HttpClient
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import whelk.Whelk
import whelk.plugin.BasicPlugin

/**
 * Created by lisa on 19/02/15.
 */
@Log
class XInfoRouteProcessor extends BasicPlugin implements Processor {

    XInfoRouteProcessor(Map settings) {
        cherryElasticUrl = settings.get("cherryElasticUrl")
        elasticCluster = settings.get("elasticCluster")
        elasticHost = settings.get("elasticHost")
        elasticPort = settings.get("elasticPort")
        indexType = settings.get("indexType")
        indexName = settings.get("indexName")
    }
    String cherryElasticUrl, elasticCluster, elasticHost, indexType, indexName = null
    int elasticPort = 9300



    @Override
    void process(Exchange exchange) throws Exception {
        if (isJsonMessage(exchange.getIn().getHeader("document:metaentry") as String)) {
            Map data = exchange.getIn().getBody(Map.class)
            def identifier = exchange.getIn().getHeader("document:identifier")
            identifier = identifier.substring(1).replaceAll(/\//, ":")

            def bookId = data.get("annotates").get("@id")
            String idField
            if (bookId.startsWith("/resource/")) {
                idField = "wasDerivedFrom.@id"

            } else {
                idField = "isbn"
                bookId = bookId.substring(9)
            }
            String operation = exchange.getIn().getHeader("whelk:operation")
            if (operation == Whelk.ADD_OPERATION) {
                operation = "INDEX"
            }
            if (operation == Whelk.BULK_ADD_OPERATION) {
                operation = "BULK_INDEX"
            }
            def parentId = findParentId(idField, bookId)
            if (parentId){
                log.info("Found a parent! $bookId -> $parentId")
                exchange.getIn().setHeader("elasticDestination", "elasticsearch://${elasticCluster}?ip=${elasticHost}&port=${elasticPort}&operation=${operation}&indexType=${indexType}&indexName=${indexName}")
                exchange.getIn().setHeader("indexId", identifier)
                exchange.getIn().setHeader("parentId", parentId)
            } else {
                log.trace("Couldn't find parent record for identifier $identifier (${bookId})")
                exchange.getIn().setHeader("elasticDestination", "stub:discard")
        }
        } else {
            log.debug("Message body is not json, sending message to stub.")
            exchange.getIn().setHeader("elasticDestination", "stub:discard")
        }
    }

    String findParentId(String idField, String value){
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(cherryElasticUrl)
        Map query = [
                "query": [
                        "term" : [ (idField) : value ]
                ]
        ]
        post.setEntity(new StringEntity(mapper.writeValueAsString(query)))
        def response = client.execute(post)
        def json = mapper.readValue(response.entity.getContent(), Map.class)
        try {
            return json.get('hits').get('hits').first().get('_id')
        } catch (NoSuchElementException nsee) {
            return null
        }
    }

    boolean isJsonMessage(String json) {
        def metaentry = mapper.readValue(json, Map)
        def ctype = metaentry.entry.contentType
        ctype ==~ /application\/(\w+\+)*json/ || ctype ==~ /application\/x-(\w+)-json/
    }
}
