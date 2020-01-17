package whelk.component

import groovy.json.JsonOutput
import groovy.util.logging.Log4j2 as Log
import org.apache.commons.codec.binary.Base64
import org.codehaus.jackson.map.ObjectMapper
import se.kb.libris.utils.isbn.ConvertException
import se.kb.libris.utils.isbn.Isbn
import se.kb.libris.utils.isbn.IsbnException
import se.kb.libris.utils.isbn.IsbnParser
import whelk.Document
import whelk.JsonLd
import whelk.Whelk

import java.util.concurrent.LinkedBlockingQueue

@Log
class ElasticSearch {
    static final String BULK_CONTENT_TYPE = "application/x-ndjson"
    private static final ObjectMapper mapper = new ObjectMapper()

    String defaultIndex = null
    private List<String> elasticHosts
    private String elasticCluster
    private ElasticClient client

    private class RetryEntry {
        Document doc
        String collection
        Whelk whelk
    }

    private final Queue<RetryEntry> indexingRetryQueue = new LinkedBlockingQueue<>()

    ElasticSearch(Properties props) {
        this(
                props.getProperty("elasticHost"),
                props.getProperty("elasticCluster"),
                props.getProperty("elasticIndex")
        )
    }

    ElasticSearch(String elasticHost, String elasticCluster, String elasticIndex) {
        this.elasticHosts = getElasticHosts(elasticHost)
        this.elasticCluster = elasticCluster
        this.defaultIndex = elasticIndex

        client = ElasticClient.withDefaultHttpClient(elasticHosts)

        new Timer("ElasticIndexingRetries", true).schedule(new TimerTask() {
            void run() {
                indexingRetryQueue.size().times {
                    RetryEntry entry = indexingRetryQueue.poll()
                    if (entry != null)
                        index(entry.doc, entry.collection, entry.whelk)
                }
            }
        }, 60*1000, 10*1000)
    }

    private List<String> getElasticHosts(String elasticHost) {
        List<String> hosts = []
        for (String host : elasticHost.split(",")) {
            host = host.trim()
            if (!host.contains(":"))
                host += ":9200"
            hosts.add("http://" + host)
        }
        return hosts
    }

    String getIndexName() { defaultIndex }

	/**
	 * Get ES mappings for associated index
	 *
	 */
	Map getMappings() {
		Tuple2<Integer, String> res = client.performRequest('GET', "/${indexName}/_mappings", '')
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

    void bulkIndex(List<Document> docs, String collection, Whelk whelk) {
        assert collection
        if (docs) {
            String bulkString = docs.collect{ doc ->
                try {
                    String shapedData = JsonOutput.toJson(
                        getShapeForIndex(doc, whelk, collection))
                    String action = createActionRow(doc, collection)
                    return "${action}\n${shapedData}\n"
                } catch (Exception e) {
                    log.error("Failed to index ${doc.getShortId()} in elastic.", e)
                    throw e
                }
            }.join('')

            String response = client.performRequest('POST', '/_bulk', bulkString, BULK_CONTENT_TYPE).second
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
            def response = client.performRequest('PUT',
                    "/${indexName}/${collection}" +
                            "/${toElasticId(doc.getShortId())}?pipeline=libris",
                    JsonOutput.toJson(shapedData)).second
            Map responseMap = mapper.readValue(response, Map)
            log.debug("Indexed the document ${doc.getShortId()} as ${indexName}/${collection}/${responseMap['_id']} as version ${responseMap['_version']}")
        } catch (Exception e) {
            log.error("Failed to index ${doc.getShortId()} in elastic, placing in retry queue.", e)
            indexingRetryQueue.add(new RetryEntry(doc: doc, collection: collection, whelk: whelk))
        }
    }

    void remove(String identifier) {
        log.debug("Deleting object with identifier ${toElasticId(identifier)}.")
        def dsl = ["query":["term":["_id":toElasticId(identifier)]]]
        def response = client.performRequest('POST',
                "/${indexName}/_delete_by_query?conflicts=proceed",
                JsonOutput.toJson(dsl)).second
        Map responseMap = mapper.readValue(response, Map)
        log.debug("Response: ${responseMap.deleted} of ${responseMap.total} " +
                  "objects deleted")
    }

    Map getShapeForIndex(Document document, Whelk whelk, String collection) {
        Document copy = document.clone()

        if (!collection.equals("hold")) {
            embellish(whelk, document, copy)
        }

        log.debug("Framing ${document.getShortId()}")
        boolean chipsify = false
        boolean addSearchKey = true
        copy.data['@graph'] = copy.data['@graph'].collect { whelk.jsonld.toCard(it, chipsify, addSearchKey) }

        setComputedProperties(copy, whelk)
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

    void embellish(Whelk whelk, Document document, Document copy) {
        boolean filterOutNonChipTerms = true // Consider using false here, since cards-in-cards work now.
        whelk.embellish(copy, filterOutNonChipTerms)
    }

    private static void setComputedProperties(Document doc, Whelk whelk) {
        getOtherIsbns(doc.getIsbnValues())
                .each { doc.addTypedThingIdentifier('ISBN', it) }

        getOtherIsbns(doc.getIsbnHiddenValues())
                .each { doc.addIndirectTypedThingIdentifier('ISBN', it) }

        doc.data['@graph'][1]['reverseLinks'] = [
                (JsonLd.TYPE_KEY) : 'PartialCollectionView',
                'totalItems' : whelk.getStorage().getDependers(doc.getShortId()).size()]
    }

    private static Collection<String> getOtherIsbns(List<String> isbns) {
        isbns.findResults { getOtherIsbnForm(it) }
                .findAll { !isbns.contains(it) }
    }

    private static String getOtherIsbnForm(String isbnValue) {
        Isbn isbn
        try {
            isbn = IsbnParser.parse(isbnValue)
        } catch (IsbnException e) {
            log.warn "Could not parse ISBN ${isbnValue}: $e"
        }
        if (isbn == null) {
            //Isbnparser.parse() returns null for invalid ISBN forms
            return null
        }
        def otherType = isbn.getType() == Isbn.ISBN10 ? Isbn.ISBN13 : Isbn.ISBN10
        try {
            return isbn.convert(otherType).toString()
        } catch (ConvertException ignored) {
            //Exception thrown when trying to transform non-convertible ISBN13 (starting with 979) to ISBN10
            return null
        }
    }

    Map query(Map jsonDsl, String collection) {
        return performQuery(
                jsonDsl,
                getQueryUrl(collection),
                { def d = it."_source"; d."_id" = it."_id"; return d }
        )
    }

    Map queryIds(Map jsonDsl, String collection) {
        return performQuery(
                jsonDsl,
                getQueryUrl(collection) + '?filter_path=took,hits.total,hits.hits._id',
                { it."_id" }
        )
    }

    @Override
    int hashCode() {
        return super.hashCode()
    }

    private Map performQuery(Map jsonDsl, String queryUrl, Closure<Map> hitCollector) {
        def start = System.currentTimeMillis()
        Tuple2<Integer, String> response = client.performRequest('POST',
                queryUrl,
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
        results.items = responseMap.hits.hits.collect(hitCollector)
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
