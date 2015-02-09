package whelk.component

import groovy.util.logging.Slf4j as Log

import org.elasticsearch.action.get.*
import org.elasticsearch.action.index.*
import org.elasticsearch.action.delete.*
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.search.sort.SortOrder
import static org.elasticsearch.index.query.QueryBuilders.*

import whelk.*
import whelk.exception.*


@Log
class ElasticSearchStorage extends BasicElasticComponent implements Storage {

    final static String ELASTIC_STORAGE_TYPE = "document"
    boolean versioning

    String indexName

    ElasticSearchStorage(String componentId = null, Map settings) {
        super(settings)
        this.contentTypes = settings.get('contentTypes', null)
        this.indexName = settings.get('indexName', null)
        this.versioning = settings.get('versioning', false)
        id = componentId
    }

    void componentBootstrap(String str) {
        if (!this.indexName) {
            this.indexName = str+"_"+this.id
        }
        log.info("Elastic Storage using index name $indexName")
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
    boolean store(Document doc, boolean withVersioning = versioning) {
        def olddoc = load(doc.identifier)
        if (olddoc?.checksum == doc.checksum) {
            log.debug("Supplied document already in storage.")
            return true
        }
        doc.updateModified()
        if (versioning && withVersioning) {
            performExecute(prepareIndexingRequest(doc, null, indexName))
        }
        try {
            performExecute(prepareIndexingRequest(doc, doc.identifier, indexName))
            return true
        } catch (Exception e) {
            log.error("Failed to save document ${doc?.identifier}", e)
        }
        return false
    }

    @Override
    void bulkStore(final List docs) {
        log.info("Bulk store requested. Versioning set to $versioning")
        def breq = client.prepareBulk()
        for (doc in docs) {
            def olddoc = load(doc.identifier)
            if (olddoc?.checksum == doc.checksum) {
                log.debug("Document ${doc.identifier} already in storage with same checksum.")
                continue
            }
            doc.updateModified()
            if (versioning) {
                breq.add(prepareIndexingRequest(doc, null, indexName))
            }
            breq.add(prepareIndexingRequest(doc, doc.identifier, indexName))
        }
        def response = performExecute(breq)
        if (response.hasFailures()) {
            throw new WhelkAddException("Problems with bulk store: ${response.buildFailureMessage()}")
        }
    }

    @Override
    List<Document> loadAllVersions(String identifier) {
        if (versioning) {
            def query = termQuery("identifier", identifier)
            def srq = client.prepareSearch(indexName + VERSION_STORAGE_SUFFIX).setTypes([ELASTIC_STORAGE_TYPE] as String[]).setQuery(query).addSort("entry.modified", SortOrder.ASC)
            def response = performExecute(srq)
            def docs = []
            response.hits.hits.each {
                docs << whelk.createDocumentFromJson(it.sourceAsString)
            }
            return docs
        } else {
            return [get(identifier)]
        }
    }

    protected prepareIndexingRequest(doc, identifier, idxName) {
        String encodedIdentifier = toElasticId(doc.identifier)
        if (identifier) {
            return client.prepareIndex(idxName, ELASTIC_STORAGE_TYPE, encodedIdentifier).setSource(doc.toJson().getBytes("UTF-8"))
        } else {
            String idAndChecksum = encodedIdentifier + ":" + doc.checksum
            log.debug("Saving versioned document with identifier $idAndChecksum")
            return client.prepareIndex(idxName + VERSION_STORAGE_SUFFIX, ELASTIC_STORAGE_TYPE, idAndChecksum).setSource(doc.toJson().getBytes("UTF-8"))
        }
    }

    Document load(String identifier) {
        return load(identifier, null)
    }

    @Override
    Document load(String identifier, String version) {
        Document document = null
        def grq = client.prepareGet(indexName, ELASTIC_STORAGE_TYPE, toElasticId(identifier))
        def response = grq.execute().actionGet();


        int v = -1
        if (version && version.isInteger()) {
            v = version.toInteger()
        }


        if (response.exists) {
            log.trace("Get response for ${identifier}: " + response.sourceAsMap)
            document = whelk.createDocumentFromJson(response.sourceAsString)
        }

        if (document && (v < 1 || version == document.checksum)) {
            return document
        } else if (document != null) {
            def docList = loadAllVersions(identifier)
            if (v > 0 && v < docList.size()) {
                document = docList[v]
            } else {
                document = docList.find { it.checksum == version }
            }
        }
        return document
    }

    @Override
    Document loadByAlternateIdentifier(String identifier) {
        def query = termQuery("entry.alternateIdentifiers", identifier)
        def srq = client.prepareSearch(indexName).setTypes([ELASTIC_STORAGE_TYPE] as String[]).setQuery(query).setSize(1)
        def response = performExecute(srq)
        log.trace("Response from alternate identifiers search: $response")
        if (response.hits.totalHits == 1) {
            return whelk.createDocumentFromJson(response.hits.hits[0].sourceAsString)
        }
        return null
    }

    Iterable<Document> loadAll(String dataset = null, Date since = null, Date until = null) {
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
                                    try {
                                        results.add(whelk.createDocumentFromJson(it.sourceAsString))
                                    } catch (Exception e) {
                                        log.error("Failed to deserialize document ${it.id} ${e.message}")
                                    }
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
    void remove(String identifier, String dataset) {
        if (!versioning) {
            log.debug("Deleting record at $indexName with id ${toElasticId(identifier)}")
            client.delete(new DeleteRequest(indexName, ELASTIC_STORAGE_TYPE, toElasticId(identifier)))
        } else {
            log.debug("Creating tombstone record at $indexName with id ${toElasticId(identifier)}")
            store(createTombstone(identifier, dataset))
        }
    }
}
