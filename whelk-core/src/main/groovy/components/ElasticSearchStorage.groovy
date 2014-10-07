package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import org.elasticsearch.action.get.*
import org.elasticsearch.action.index.*
import org.elasticsearch.action.delete.*
import org.elasticsearch.common.unit.TimeValue
import static org.elasticsearch.index.query.QueryBuilders.*

import org.apache.camel.*
import org.apache.camel.impl.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.exception.*


@Log
class ElasticSearchStorage extends BasicElasticComponent implements Storage {

    final static String ELASTIC_STORAGE_TYPE = "document"
    final static String VERSION_STORAGE_SUFFIX = "_versions"
    boolean versioning

    String indexName

    private ElasticShapeComputer shapeComputer

    ElasticSearchStorage(Map settings) {
        super(settings)
        this.contentTypes = settings.get('contentTypes', null)
        this.indexName = settings.get('indexName', null)
        this.versioning = settings.get('versioning', false)
    }

    void componentBootstrap(String str) {
        if (!this.indexName) {
            this.indexName = this.id
        }
        log.info("Elastic Storage using index name $indexName")
        shapeComputer = plugins.find { it instanceof ElasticShapeComputer }
        assert shapeComputer
    }

    @Override
    void onStart() {
        log.info("Starting ${this.id} with index name $indexName")
        createIndexIfNotExists(indexName, true)
        checkTypeMapping(indexName, ELASTIC_STORAGE_TYPE)
        if (versioning) {
            createIndexIfNotExists(indexName + VERSION_STORAGE_SUFFIX, true)
            checkTypeMapping(indexName + VERSION_STORAGE_SUFFIX, ELASTIC_STORAGE_TYPE)
        }
    }

    @Override
    boolean store(Document doc) {
        if (versioning) {
            def oldDoc = null
            (oldDoc, doc) = fetchAndUpdateVersion(doc)
            if (oldDoc) {
                log.debug("Saving old version with version ${oldDoc.version}.")
                performExecute(prepareIndexingRequest(oldDoc, null, indexName + VERSION_STORAGE_SUFFIX))
            }
        }
        log.debug("Saving doc (${doc.identifier}) with version ${doc.version}")
        performExecute(prepareIndexingRequest(doc, doc.identifier, indexName))
        whelk.notifyCamel(doc, [:])
        return true
    }

    @Override
    void bulkStore(List docs) {
        def breq = client.prepareBulk()
        for (doc in docs) {
            doc.updateTimestamp()
            if (versioning) {
                def oldDoc = null
                (oldDoc, doc) = fetchAndUpdateVersion(doc)
                if (oldDoc) {
                    log.debug("Saving old version with version ${oldDoc.version}.")
                    breq.add(prepareIndexingRequest(oldDoc, null, indexName + VERSION_STORAGE_SUFFIX))
                }
            }
            breq.add(prepareIndexingRequest(doc, doc.identifier, indexName))
            whelk.notifyCamel(doc, [:])
        }
        def response = performExecute(breq)
        if (response.hasFailures()) {
            throw new WhelkAddException("Problems with bulk store: ${response.buildFailureMessage()}")
        }
    }

    protected fetchAndUpdateVersion(Document newDoc) {
        Document currentDoc = get(newDoc.identifier)
        if (currentDoc && currentDoc.checksum != newDoc.checksum) {
            newDoc.setVersion(currentDoc.version + 1)
        } else {
            currentDoc = null
        }
        return [currentDoc, newDoc]
    }

    protected prepareIndexingRequest(doc, identifier, index) {
        if (identifier) {
            String encodedIdentifier = shapeComputer.translateIdentifier(new URI(doc.identifier))
            return client.prepareIndex(indexName, ELASTIC_STORAGE_TYPE, encodedIdentifier).setSource(doc.toJson().getBytes("UTF-8"))
        } else {
            return client.prepareIndex(indexName + VERSION_STORAGE_SUFFIX, ELASTIC_STORAGE_TYPE).setSource(doc.toJson().getBytes("UTF-8"))
        }
    }

    Document get(String identifier) {
        get(new URI(identifier))
    }

    @Override
    Document get(URI uri, String version = null) {
        def grq = client.prepareGet(indexName, ELASTIC_STORAGE_TYPE, shapeComputer.translateIdentifier(uri))
        def response = grq.execute().actionGet();

        int v = (version ? version as int : -1)

        if (!response.exists) {
            return null
        }
        log.trace("Get response for ${uri.toString()}: " + response.sourceAsMap)
        Document document = Document.fromJson(response.sourceAsString)
        if (document && (v < 0 || document.version == v)) {
            return document
        } else {
            log.debug("Current version (${document.version}) of document not the one requested ($v). Looking in the cellar ...")
            def query = boolQuery().must(termQuery("identifier", uri.toString())).must(termQuery("entry.version", v))
            def srq = client.prepareSearch(indexName + VERSION_STORAGE_SUFFIX).setTypes([ELASTIC_STORAGE_TYPE] as String[]).setQuery(query).setSize(1)
            response = performExecute(srq)
            log.trace("Response from version search: $response")
            if (response.hits.totalHits == 1) {
                return Document.fromJson(response.hits.hits[0].sourceAsString)
            }
        }
        return null
    }

    Iterable<Document> getAll(String dataset = null, Date since = null, Date until = null) {
        return new Iterable<Document>() {
            def results = []
            Iterator<Document> iterator() {
                Iterator listIterator = null

                return new Iterator<Document>() {

                    String token = null

                    public boolean hasNext() {
                        if (results.isEmpty()) {
                            listIterator = null
                            log.debug("Getting results from elastic.")
                            def srb
                            if (!token) {
                                log.debug("Starting matchAll-query")
                                srb = client.prepareSearch(indexName)
                                srb = srb.setTypes(ELASTIC_STORAGE_TYPE).setScroll(TimeValue.timeValueMinutes(20)).setSize(batchUpdateSize)
                                if (dataset || since || until) {
                                    def query = boolQuery()
                                    if (dataset) {
                                        query = query.must(termQuery("entry.dataset", dataset))
                                    }
                                    if (since || until) {
                                        def timeRangeQuery = rangeQuery("entry.timestamp")
                                        if (since) {
                                            timeRangeQuery = timeRangeQuery.from(since.getTime())
                                        }
                                        if (until) {
                                            timeRangeQuery = timeRangeQuery.to(until.getTime())
                                        }
                                        query = query.must(timeRangeQuery)
                                    }
                                    srb.setQuery(query)
                                } else {
                                    srb.setQuery(matchAllQuery())
                                }
                            } else {
                                log.debug("Continuing query with scrollId $token")
                                srb = client.prepareSearchScroll(token).setScroll(TimeValue.timeValueMinutes(2))
                            }
                            log.debug("Query is: " + srb)
                            def response = performExecute(srb)
                            log.trace("Response: " + response)

                            if (response) {
                                log.debug "Total hits: ${response.hits.totalHits}"
                                response.hits.hits.each {
                                    results.add(Document.fromJson(it.sourceAsString))
                                }
                                log.debug("Found " + results.size() + " items. Scroll ID: " + response.scrollId)
                                token = response.scrollId
                            } else if (!response || response.hits.length < 1) {
                                log.debug("No response recevied.")
                                token = null
                            }
                            listIterator = results.iterator()
                        }
                        return !results.isEmpty()
                    }
                    public Document next() {
                        if (listIterator == null) {
                            listIterator = results.iterator()
                        }
                        Document d = listIterator.next()
                        if (!listIterator.hasNext()) {
                            listIterator = null
                            results = new LinkedHashSet<Document>()
                        }
                        log.trace("Got document ${d?.identifier}")
                        return d
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                }
            }
        }
    }

    @Override
    void remove(URI id) {
        if (!versioning) {
            client.delete(new DeleteRequest(indexName, ELASTIC_STORAGE_TYPE, shapeComputer.translateIdentifier(id)))
        } else {
            store(createTombstone(id))
        }
    }

    private Document createTombstone(uri) {
        def tombstone = new Document().withIdentifier(uri).withData("DELETED ENTRY")
        tombstone.entry['deleted'] = true
        tombstone.updateTimestamp()
        return tombstone
    }
}
