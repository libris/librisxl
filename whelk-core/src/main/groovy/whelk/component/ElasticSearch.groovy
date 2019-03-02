package whelk.component

import groovy.json.JsonOutput
import groovy.util.logging.Log4j2 as Log

import org.apache.commons.codec.binary.Base64
import org.apache.http.entity.ContentType
import org.codehaus.jackson.map.ObjectMapper
import whelk.util.LegacyIntegrationTools
import whelk.util.LongTermHttpConnection
import whelk.Document
import whelk.JsonLd
import whelk.exception.*
import whelk.Whelk

import java.util.concurrent.atomic.AtomicBoolean

@Log
class ElasticSearch {

    private class ConnectionPoolEntry {
        public ConnectionPoolEntry(LongTermHttpConnection connection) {
            this.connection = connection
            this.inUse = new AtomicBoolean(false)
        }
        AtomicBoolean inUse
        LongTermHttpConnection connection
    }

    static final String BULK_CONTENT_TYPE = "application/x-ndjson"
    static final int CONNECTION_POOL_SIZE = 9

    Vector<ConnectionPoolEntry> httpConnectionPool = []
    String defaultIndex = null
    private List<String> elasticHosts
    private String elasticCluster

    private static final ObjectMapper mapper = new ObjectMapper()

    ElasticSearch(Properties props) {
        this.elasticHosts = getElasticHosts(props.getProperty("elasticHost"))
        this.elasticCluster = props.getProperty("elasticCluster")
        this.defaultIndex = props.getProperty("elasticIndex")
        setup()
    }

    ElasticSearch(String elasticHost, String elasticCluster, String elasticIndex) {
        this.elasticHosts = getElasticHosts(elasticHost)
        this.elasticCluster = elasticCluster
        this.defaultIndex = elasticIndex
        setup()
    }

    private List<String> getElasticHosts(String elasticHost) {
        List<String> hosts = []
        for (String host : elasticHost.split(",")) {
            if (!host.contains(":"))
                host += ":9200"
            hosts.add("http://" + host)
        }
        return hosts
    }

    private void setup() {
        for (int i = 0; i < CONNECTION_POOL_SIZE; ++i) {
            String host = elasticHosts[ i % elasticHosts.size() ]
            httpConnectionPool.add(new ConnectionPoolEntry(new LongTermHttpConnection(host)))
        }
        log.info "ElasticSearch component initialized with ${elasticHosts.count{it}} nodes and $CONNECTION_POOL_SIZE workers."
     }

    String getIndexName() { defaultIndex }

	/**
	 * Get ES mappings for associated index
	 *
	 */
	Map getMappings() {
		Tuple2<Integer, String> res = performRequest('GET', "/${indexName}/_mappings", '')
		int statusCode = res.first
		if (statusCode != 200) {
			log.warn("Got unexpected status code ${statusCode} when getting ES mappings.")
			return null
		}
		String responseBody = res.second
		Map response =  mapper.readValue(responseBody, Map)

        // Since ES aliases return the name of the index rather than the alias,
        // we don't rely on names here.
        List<String> keys = response.keySet() as List

        if (keys.size() == 1 && response[(keys[0])].containsKey('mappings')) {
            return response[(keys[0])]['mappings']
        } else {
            log.warn("Couldn't get mappings from ES index ${indexName}, response was ${response}.")
            return [:]
        }
    }

    Tuple2<Integer, String> performRequest(String method, String path, String body, String contentType0 = null){

        // Get an available connection from the pool
        ConnectionPoolEntry httpConnectionEntry
        int i = 0
        while(true) {
            i++
            if (i == CONNECTION_POOL_SIZE) {
                i = 0
                Thread.yield()
            }

            synchronized (this) {
                if (!httpConnectionPool[i].inUse.get()) {
                    httpConnectionEntry = httpConnectionPool[i]
                    httpConnectionEntry.inUse.set(true)
                    break
                }
            }
        }

        Tuple2<Integer, String> response = null

        try {
            String contentType
            if (contentType0 == null) {
                contentType = ContentType.APPLICATION_JSON.toString()
            } else {
                contentType = contentType0
            }

            LongTermHttpConnection httpConnection = httpConnectionEntry.connection
            int backOffTime = 0
            while (response == null || httpConnection.getResponseCode() == 429) {
                if (backOffTime != 0) {
                    log.info("Bulk indexing request to ElasticSearch was throttled (http 429) waiting $backOffTime seconds before retry.")
                    Thread.sleep(backOffTime * 1000)
                }

                httpConnection.sendRequest(path, method, contentType, body, null, null)
                response = new Tuple2(httpConnection.responseCode, httpConnection.responseData)
                httpConnection.clearBuffers()

                if (backOffTime == 0)
                    backOffTime = 1
                else
                    backOffTime *= 2
            }
        } catch(Throwable e) {
            log.error(e)
        } finally
        {
            httpConnectionEntry.inUse.set(false)
        }

        return response
    }

    void bulkIndex(List<Document> docs, String collection, Whelk whelk) {
        assert collection
        if (docs) {
            String bulkString = docs.collect{ doc ->
                String shapedData = JsonOutput.toJson(
                    getShapeForIndex(doc, whelk, collection))
                String action = createActionRow(doc,collection)
                "${action}\n${shapedData}\n"
            }.join('')

            String response = performRequest('POST', '/_bulk', bulkString, BULK_CONTENT_TYPE).second
            Map responseMap = mapper.readValue(response, Map)
            log.info("Bulk indexed ${docs.count{it}} docs in ${responseMap.took} ms")
        }
    }

    String createActionRow(Document doc, String collection) {
        def action = ["index" : [ "_index" : indexName,
                                  "_type" : collection,
                                  "_id" : toElasticId(doc.getShortId()) ]]
        return mapper.writeValueAsString(action)
    }

    void index(Document doc, String collection, Whelk whelk) {
        // The justification for this uncomfortable catch-all, is that an index-failure must raise an alert (log entry)
        // _internally_ but be otherwise invisible to clients (If postgres writing was ok, the save is considered ok).
        try {
            Map shapedData = getShapeForIndex(doc, whelk, collection)
            //def body = new NStringEntity(JsonOutput.toJson(shapedData), ContentType.APPLICATION_JSON)
            def response = performRequest('PUT',
                    "/${indexName}/${collection}" +
                            "/${toElasticId(doc.getShortId())}?pipeline=libris",
                    JsonOutput.toJson(shapedData)).second
            //def eString = EntityUtils.toString(response.getEntity())
            Map responseMap = mapper.readValue(response, Map)
            log.debug("Indexed the document ${doc.getShortId()} as ${indexName}/${collection}/${responseMap['_id']} as version ${responseMap['_version']}")
        } catch (Exception e) {
            log.error("Failed to index ${doc.getShortId()} in elastic.", e)
        }
    }

    void remove(String identifier) {
        log.debug("Deleting object with identifier ${toElasticId(identifier)}.")
        def dsl = ["query":["term":["_id":toElasticId(identifier)]]]
        //def query = new NStringEntity(JsonOutput.toJson(dsl), ContentType.APPLICATION_JSON)
        def response = performRequest('POST',
                "/${indexName}/_delete_by_query?conflicts=proceed",
                JsonOutput.toJson(dsl)).second
        //def eString = EntityUtils.toString(response.getEntity())
        Map responseMap = mapper.readValue(response, Map)
        log.debug("Response: ${responseMap.deleted} of ${responseMap.total} " +
                  "objects deleted")
    }

    Map getShapeForIndex(Document document, Whelk whelk, String collection) {

        if (!collection.equals("hold")) {
            List externalRefs = document.getExternalRefs()
            List convertedExternalLinks = JsonLd.expandLinks(externalRefs, whelk.jsonld.getDisplayData().get(JsonLd.getCONTEXT_KEY()))
            Map referencedData = [:]
            Map externalDocs = whelk.bulkLoad(convertedExternalLinks)
            externalDocs.each { id, doc ->
                if (id && doc && doc.hasProperty('data')) {
                    referencedData[id] = doc.data
                }
                else {
                    log.warn("Could not get external doc ${id} for ${document.getShortId()}, skipping...")
                }
            }
            whelk.jsonld.embellish(document.data, referencedData, true)
        }

        log.debug("Framing ${document.getShortId()}")
        Document copy = document.clone()
        boolean chipsify = false
        copy.data['@graph'] = copy.data['@graph'].collect { whelk.jsonld.toCard(it, chipsify) }

        copy.setThingMeta(document.getCompleteId())
        List<String> thingIds = document.getThingIdentifiers()
        if (thingIds.isEmpty()) {
            log.warn("Missing mainEntity? In: " + document.getCompleteId())
            return copy.data
        }
        String thingId = thingIds.get(0)
        Map framed = JsonLd.frame(thingId, copy.data)

        log.trace("Framed data: ${framed}")

        return framed
    }

    Map query(Map jsonDsl, String collection) {
        def start = System.currentTimeMillis()
        Tuple2<Integer, String> response = performRequest('POST',
                getQueryUrl(collection),
                JsonOutput.toJson(jsonDsl))
        int statusCode = response.first
        String responseBody = response.second
        if (statusCode != 200) {
            log.warn("Unexpected response from ES: ${statusCode} ${responseBody}")
            return [:]
        }
        def duration = System.currentTimeMillis() - start
        Map responseMap = mapper.readValue(responseBody, Map)

        log.info("ES query took ${duration} (${responseMap.took} server-side)")

        def results = [:]

        results.startIndex = jsonDsl.from
        results.totalHits = responseMap.hits.total
        results.items = responseMap.hits.hits.collect { it."_source" }
        results.aggregations = responseMap.aggregations

        return results
    }

    private String getQueryUrl(String collection) {
        String maybeCollection  = ""
        if (collection) {
            maybeCollection = "${collection}/"
        }

        return "/${indexName}/${maybeCollection}_search"
    }

    static String toElasticId(String id) {
        if (id.contains("/")) {
            return Base64.encodeBase64URLSafeString(id.getBytes("UTF-8"))
        } else {
            return id // If XL-minted identifier, use the same charsequence
        }
    }
}
