package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.*
import org.elasticsearch.common.transport.*
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder
import org.elasticsearch.common.settings.*

import static org.elasticsearch.index.query.QueryBuilders.*
import static org.elasticsearch.node.NodeBuilder.*

import org.json.simple.*
import com.google.gson.*
import groovy.json.JsonSlurper

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*

import static se.kb.libris.conch.Tools.*

@Log
class ElasticSearch implements Index, Storage {

    Whelk whelk
    Client client

    boolean enabled = true
    String id = "elasticsearch"
    int MAX_TRIES = 1000

    String defaultType = "record"

    def void setWhelk(Whelk w) { this.whelk = w }

    def void enable() {this.enabled = true}
    def void disable() {this.enabled = false}

    def boolean add(Document doc) {
        boolean unfailure = false
        def dict = determineIndexAndType(doc.identifier)
        log.debug "Should use index ${dict.index}, type ${dict.type} and id ${dict.id}"
        try {
            IndexResponse response = client.prepareIndex(dict.index, dict.type, dict.id).setSource(serializeDocumentToJson(doc)).execute().actionGet()
            response = client.prepareIndex(dict.index, dict.type+":meta", dict.id).setSource(extractMetaData(doc)).execute().actionGet()
            unfailure = true
            log.debug "Indexed document with id: ${response.id}, in index ${response.index} with type ${response.type}" 
        } catch (org.elasticsearch.index.mapper.MapperParsingException me) {
            log.error("Failed to index document with id ${doc.identifier}: " + me.getMessage(), me)
            unfailure = true
        } catch (org.elasticsearch.client.transport.NoNodeAvailableException nnae) {
            log.trace("Failed to connect to elasticsearch node: " + nnae.getMessage(), nnae)
        }
        return unfailure
    }

    byte[] extractMetaData(Document doc) {
        def builder = new groovy.json.JsonBuilder()
        builder {
            "uri"(doc.identifier.toString()) 
            "version"(doc.version) 
            "contenttype"(doc.contentType) 
            "size"(doc.size)
        }
        //println "Metadata: ${builder.toString()}"
        return builder.toString().getBytes()
    }

    @Override
    void index(Document doc) {
        log.debug("Not indexing document, because we are also used as Storage. Unnecessary to store twice.")
        //add(doc.data, doc.identifier)
    }

    void delete(URI uri) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    @Override
    public void store(Document d) {
        int failcount = 0
        while (!add(d)) {
            if (failcount++ > MAX_TRIES) {
                log.error("Failed to store document after $MAX_TRIES attempts")
                break;
            }
            Thread.sleep(100)
        }
    }

    OutputStream getOutputStreamFor(Document doc) {
        log.debug("Preparing outputstream for document ${doc.identifier}")
            return new ByteArrayOutputStream() {
                void close() throws IOException {
                    doc = doc.withData(toByteArray())
                    ElasticSearch.this.add(doc)
                }
            }
    }

    boolean isJSON(data) {
        Gson gson = new Gson()
        try {
            gson.fromJson(new String(data), Object.class)
        } catch (JsonSyntaxException jse) {
            log.debug("Data was not appliction/json")
            return false
        }
        return true
    }

    def determineIndexAndType(URI uri) {
        def pathparts = uri.path.split("/").reverse()
        int maxpart = pathparts.size() - 2
        def dict = [:]
        def typeelements = []
        pathparts.eachWithIndex() { part, i ->
            if (i == 0) {
                dict['id'] = part
            } else if (i == maxpart) {
                dict['index'] = part
            } else if (i < maxpart) {
                typeelements.add(part)
            }
        }
        if (!dict['index']) {
            dict['index'] = whelk.defaultIndex
        }
        def type = typeelements.join("_")
        dict['type'] = (type ? type : this.defaultType)
        return dict
    }

    private GetResponse getFromElastic(index, type, id) {
        GetResponse response  = null
        try {
            response = client.prepareGet(index, type, id).execute().actionGet()
        } catch (Exception e) { 
            log.debug("Failed to get response from server.", e)
        }
        return response
    }

    @Override
    Document get(URI uri, raw = false) {
        def dict = determineIndexAndType(uri)
        GetResponse response = null
        GetResponse metaresponse = null
        int failcount = 0
        while (response == null) {
            response = getFromElastic(dict['index'], dict['type'], dict['id'])
            if (response == null) {
                log.debug("Retrying server connection ...")
                if (failcount++ > MAX_TRIES) {
                    log.error("Failed to connect to elasticsearch after $MAX_TRIES attempts.")
                    break
                }
                Thread.sleep(100)
            } 
        }
        if (response && response.exists()) {
            Document d 
            def map = response.sourceAsMap()
            log.debug("Raw mode: ${raw}")
            /*
            map.each {
            log.debug("MAP: " + it.key +":" + it.value)
            }
            */
            if (raw) {
                d = this.whelk.createDocument().withIdentifier(uri).withContentType("application/json").withData(response.source())
            } else {
                d = deserializeJsonDocument(response.source(), uri, getFromElastic(dict['index'], dict['type']+":meta", dict['id']))
            }

            //println "Returning document with ctype ${d.contentType}"
            return d
        }
        return null
    }

    private SearchResponse performQuery(query, index) {
        SearchResponse response = null
        try {
            def srb
            if (index == null) {
                srb = client.prepareSearch()  
            } else {
                srb = client.prepareSearch(index)  
            }
            response = srb
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setQuery(queryString(query))
            //.setFrom(0).setSize(60)
            .setExplain(true)
            .execute()
            .actionGet()
        } catch (NoNodeAvailableException e) {
            log.debug("Failed to get response from server.", e)
        } 
        return response
    }

    @Override
    SearchResult query(String query) { //, boolean raw = false) {
        log.debug "Doing query on $query"
        def index = whelk.name
        def response = null
        int failcount = 0
        while (!response) {
             response = performQuery(query, index)
             if (!response) {
                 log.debug("Retrying server connection ...")
                 if (failcount++ > MAX_TRIES) {
                    log.error("Failed to connect to elasticsearch after $MAX_TRIES attempts.")
                     break
                 }
             }
        }
        def results = new BasicSearchResult()
        if (response) {
            log.debug "Total hits: ${response.hits.totalHits}"
            response.hits.hits.each {
                results.addHit(this.whelk.createDocument().withData(it.source()))
            }
        }
        /*
        if (raw) {
            return new BasicSearchResult(response.toString(), response.hits.totalHits)
            //return response.toString()
        } else {
            def hits = new StringBuilder()
            response.hits.hits.eachWithIndex() { it, i ->
                if (i > 0) { hits << "," }
                hits << new String(it.source())
            }
            hits = hits.insert(0,"[")
            hits = hits.append("]")
            return new BasicSearchResult(hits.toString(), response.hits.totalHits)
        }
        */
        return results
    }

    @Override
    LookupResult lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    Document deserializeJsonDocument(data, uri, metaresponse) {
        def version, size
        def contentType = "application/json"
        if (metaresponse && metaresponse.exists()) {
            def slurper = new JsonSlurper()
            def slurped = slurper.parseText(new String(data))
            version = slurped.version
            size = slurped.size
            contentType = (slurped.contenttype ? slurped.contenttype : contentType)
        }
        Document doc = this.whelk.createDocument().withData(data).withIdentifier(uri).withContentType(contentType)
        return doc
    }

    def serializeDocumentToJson(Document doc) {
        if (doc.contentType != "application/json") {
            log.debug("Document is not JSON, must wrap it.")
            Gson gson = new Gson()
            def docrepr = [:]
            docrepr['data'] = new String(doc.data)
            docrepr['uri'] = doc.identifier
            docrepr['contenttype'] = "text/plain"
            docrepr['size'] = doc.size
            docrepr['version'] = doc.version
            return gson.toJson(docrepr)
        }
        return doc.data
    }
}

@Log
class ElasticSearchClient extends ElasticSearch {

    def ElasticSearchClient() {
        Properties properties = new Properties();
        def is = ElasticSearchClient.class.getClassLoader().getResourceAsStream("whelks-core.properties")
        properties.load(is);
        final String elastichost = properties.getProperty("elastichost");

        log.debug "Connecting to elastichost:9300"
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.ping_timeout", 30).build();
        client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(elastichost, 9300))
        log.debug("... connected")
    }
} 

@Log
class ElasticSearchNode extends ElasticSearch {

    def ElasticSearchNode() {
        log.debug "Creating elastic node"
        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder()
        // here you can set the node and index settings via API
        settings.build()
        NodeBuilder nBuilder = nodeBuilder().settings(settings)
        //
        // start it!
        def node = nBuilder.build().start()
        client = node.client()
        log.debug "Client connected to new ES node"
    }
}
