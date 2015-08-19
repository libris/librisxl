package whelk.plugin.libris

import groovy.util.logging.Slf4j as Log

import whelk.*
import whelk.plugin.*
import whelk.component.*
import whelk.exception.*

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
class JsonLDLinkExpander extends BasicFilter implements WhelkAware {

    Map nodesToExpand = null
    private Map cachedDocuments = [:]
    Map settings
    Whelk whelk
    Index index

    PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager()
    HttpClient client = HttpClients.custom().setConnectionManager(cm).build()

    private static final JsonLDLinkExpander instance = new JsonLDLinkExpander()
    private JsonLDLinkExpander() {}
    public static JsonLDLinkExpander getInstance() { return instance }

    public void setSettings(Map settings) {
        this.nodesToExpand = settings.get('nodesToExpand').asImmutable()

    }

    /**
     * Keys to preserve from linked document.
     */
    private List getRelevantKeys(String identifier) {
        if (identifier.startsWith("/def/enum/")) {
            return ["@id", "@type", "prefLabel"]
        }
        return null
    }

    @Override
    boolean valid(Document doc) {
        if (doc && doc.isJson() && doc.contentType == "application/ld+json" && this.nodesToExpand.containsKey(doc.manifest.dataset)) {
            return true
        }
        return false
    }

    @groovy.transform.Synchronized
    void loadCachedDocuments() {
        log.debug("Caching def-documents.")
        for (doc in whelk.loadAll("def")) {
            def dataMap = (doc.dataAsMap.containsKey("about") ? doc.dataAsMap.get("about") : doc.dataAsMap)
            dataMap.put("@id", doc.identifier)
            if (getRelevantKeys(doc.identifier)) {
                def newMap = [:]
                for (key in getRelevantKeys(doc.identifier)) {
                    if (dataMap.containsKey(key)) {
                        newMap.put(key, dataMap.get(key))
                    }
                }
                dataMap = newMap
            }
            log.debug("adding ${doc.identifier} ($dataMap) to cache")
            cachedDocuments.put(doc.identifier, dataMap)
        }
        log.debug("Cached ${cachedDocuments.size()} docs.")
    }

    Document doFilter(Document doc) {
        log.debug("Expanding ${doc.identifier}")
        def dataMap = doFilter(doc.dataAsMap, doc.dataset)
        return doc.withData(dataMap)
    }

    Map doFilter(Map dataMap, String dataset) {
        if (!cachedDocuments) {
            loadCachedDocuments()
        }
        nodesToExpand[dataset].each { key, instructions ->
            log.trace("key: $key, instructions: $instructions")
            def mapSegment = getNestedObject(key, dataMap)
            if (mapSegment instanceof List) {
                int i = 0
                for (map in mapSegment) {
                    mapSegment[i] = expandNode(map, instructions)
                    i++
                }
            } else if (mapSegment instanceof Map) {
                log.trace("trying to replace mapsegment $mapSegment")
                def expandedNode = expandNode(mapSegment, instructions)
                setNestedObject(key, expandedNode, dataMap)
            } else if (mapSegment == null) {
                log.trace("Path $key not available.")
                return
            } else {
                throw new WhelkRuntimeException("The path key \"$key\" does not point to a Map or a List. Please check configuration.")
            }
        }
        return dataMap
    }

    Index getIndex() {
        if (!index) {
            index = plugins.find { it instanceof Index }
        }
        return index
    }

    Map expandNode(Map node, Map instructions) {
        if (cachedDocuments.containsKey(node['@id'])) {
            return cachedDocuments.get(node['@id'])
        } else if (node['@id'] && !node['@id'].startsWith("/def/")) {
            if (instructions['method'] == "query") {
                Map queryMap = [ 'terms': [ instructions['field']+":"+node['@id'] ] ]
                if (instructions['queryProperties']) {
                    if (instructions['queryProperties']['_source.include'] && !instructions['queryProperties']['_source.include'].contains(instructions['field'])) {
                        instructions['queryProperties']['_source.include'] << instructions['field']
                    }
                    queryMap.putAll(instructions['queryProperties'])
                }
                def query = new ElasticQuery(queryMap)
                def result = getIndex().query(query, this.whelk.index.getIndexName(), instructions['dataset'])
                if (result.numberOfHits) {
                    log.trace("data is : " + result.hits[0].dataAsString)
                    log.trace("ct is : " + result.hits[0].contentType)

                    def resultMap = (instructions['resultKey'] ? result.hits[0].dataAsMap.get(instructions['resultKey']) : result.hits[0].dataAsMap)
                    def bastards = (instructions['resultKey'] ? instructions['queryProperties']['_source.include'].findAll { it != "@id" && !it.startsWith(instructions['resultKey'])} : [])
                    if (bastards.size() > 0) {
                        bastards.each {
                            resultMap.put("@reverse", [(instructions['resultKey']): [(it): result.hits[0].dataAsMap.get(it)]])
                        }
                    }

                    return resultMap
                } else {
                    log.trace("No results in query.")
                    return node
                }
            }
            if (instructions['method'] == "get") {
                log.debug("getting from whelk for node $node")
                def doc = whelk.get(node['@id'], Storage.RAW_VERSION)
                return (doc ? doc.dataAsMap : node)
            }
            if (instructions['method'] == "http") {
                HttpGet get = new HttpGet(new URI(instructions['url']+node['@id']))
                def response = client.execute(get)
                log.trace("response: ${response.getEntity().getContent().getText()}")
                // TODO: implement
            }
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
