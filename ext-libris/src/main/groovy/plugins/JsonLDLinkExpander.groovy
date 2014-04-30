package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*

import org.apache.http.client.*
import org.apache.http.client.methods.*
import org.apache.http.client.utils.*
import org.apache.http.entity.*
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.message.BasicNameValuePair
import org.apache.http.impl.conn.*
import org.apache.http.impl.client.*
import org.apache.http.client.protocol.*


@Log
class JsonLDLinkExpander extends BasicLinkExpander implements WhelkAware {

    Map nodesToExpand
    List requiredDataset
    Whelk whelk

    PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager()
    HttpClient client = HttpClients.custom().setConnectionManager(cm).build()

    JsonLDLinkExpander(Map settings) {
        this.nodesToExpand = settings.get('nodesToExpand')
        this.requiredDataset = settings.get('requiredDataset')
    }

    boolean valid(Document doc) {
        if (doc && doc.isJson() && doc.contentType == "application/ld+json" && this.requiredDataset.contains(doc.entry.dataset)) {
            return true
        }
        return false
    }

    Document doExpand(Document doc) {
        log.debug("Expanding ${doc.identifier}")
        def dataMap = doc.dataAsMap
        nodesToExpand.each { key, instructions ->
            log.debug("key: $key, instructions: $instructions")
            def mapSegment = getNestedObject(key, dataMap)
            if (mapSegment instanceof List) {
                int i = 0
                for (map in mapSegment) {
                    mapSegment[i] = expandNode(map, instructions)
                    i++
                }
            } else if (mapSegment instanceof Map) {
                log.debug("trying to replace mapsegment $mapSegment")
                def expandedNode = expandNode(mapSegment, instructions)
                setNestedObject(key, expandedNode, dataMap)
            } else if (mapSegment == null) {
                log.debug("Path $key not available in ${doc.identifier}")
                return
            } else {
                throw new WhelkRuntimeException("The path key \"$key\" in ${doc.identifier} does not point to a Map or a List. Please check configuration.")
            }
        }
        return doc.withData(dataMap)
    }

    Map expandNode(Map node, Map instructions) {
        if (instructions['method'] == "query") {
            Map queryMap = [ 'terms': [ instructions['field']+":"+node['@id'] ] ]
            if (instructions['queryProperties']) {
                queryMap.putAll(instructions['queryProperties'])
            }
            def query = new ElasticQuery(queryMap)
            def result = whelk.index.query(query, instructions['index'], instructions['dataset'])
            if (result.numberOfHits) {
                log.trace("data is : " + result.hits[0].dataAsString)
                log.trace("ct is : " + result.hits[0].contentType)
                return  (instructions['resultKey'] ? result.hits[0].dataAsMap.get(instructions['resultKey']) : result.hits[0].dataAsMap)
            } else {
                log.debug("No results for $queryMap")
                return node
            }
        }
        if (instructions['method'] == "get") {
            log.debug("getting from whelk for node $node")
            def doc = whelk.get(new URI(node['@id']))
            return (doc ? doc.dataAsMap : node)
        }
        if (instructions['method'] == "http") {
            HttpGet get = new HttpGet(new URI(instructions['url']+node['@id']))
            def response = client.execute(get)
            log.trace("response: ${response.getEntity().getContent().getText()}")
            // TODO: implement
        }
        return node
    }

    def getNestedObject(key, map) {
        def m = map
        for (k in key.split(/\./)) {
            if (m && m.containsKey(k)) {
                m = m[k]
            } else {
                return null
            }
        }
        return m
    }

    void setNestedObject(key, node, dataMap) {
        def m = dataMap
        def elems = key.split(/\./)
        for (int i=0; i < elems.size(); i++) {
            if (i+1 == elems.size()) {
                m.put(elems[i], node)
            } else {
                m = m.get(elems[i])
            }
        }
    }
}
