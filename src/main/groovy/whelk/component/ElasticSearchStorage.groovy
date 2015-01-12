package whelk.component

import groovy.util.logging.Slf4j as Log

import org.elasticsearch.action.get.*
import org.elasticsearch.action.index.*
import org.elasticsearch.action.delete.*
import org.elasticsearch.common.unit.TimeValue
import static org.elasticsearch.index.query.QueryBuilders.*

import whelk.*
import whelk.exception.*


@Log
class ElasticSearchStorage extends BasicElasticComponent implements Storage {

    final static String ELASTIC_STORAGE_TYPE = "document"
    final static String VERSION_STORAGE_SUFFIX = "_versions"
    boolean versioning

    String indexName

    ElasticSearchStorage(Map settings) {
        super(settings)
        this.contentTypes = settings.get('contentTypes', null)
        this.indexName = settings.get('indexName', null)
        this.versioning = settings.get('versioning', false)
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
    boolean eligibleForStoring(Document doc) {
        if (versioning) {
            Document currentDoc = get(doc.identifier)
            log.debug("eligible: $currentDoc - ${currentDoc?.deleted}, checksums: ${currentDoc?.checksum} / ${doc.checksum}")
            if (currentDoc && !currentDoc.isDeleted() && currentDoc.checksum == doc.checksum) {
                log.debug("Document ${doc.identifier} is not suitable for storing.")
                return false
            }
        }
        log.debug("Document ${doc.identifier} is deemed eligible for storing.")
        return true
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
        def breq = client.prepareBulk()
        for (doc in docs) {
            if (versioning) {
                def oldDoc = null
                (oldDoc, doc) = fetchAndUpdateVersion(doc)
                if (oldDoc) {
                    log.debug("Saving old version with version ${oldDoc.version}.")
                    breq.add(prepareIndexingRequest(oldDoc, null, indexName + VERSION_STORAGE_SUFFIX))
                }
            }
            breq.add(prepareIndexingRequest(doc, doc.identifier, indexName))
        }
        def response = performExecute(breq)
        if (response.hasFailures()) {
            throw new WhelkAddException("Problems with bulk store: ${response.buildFailureMessage()}")
        }
    }

    List<Document> getAllVersions(String identifier) {
        if (versioning) {
            def query = termQuery("identifier", identifier)
            def srq = client.prepareSearch(indexName + VERSION_STORAGE_SUFFIX).setTypes([ELASTIC_STORAGE_TYPE] as String[]).setQuery(query)
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

    protected fetchAndUpdateVersion(Document newDoc) {
        Document currentDoc = get(newDoc.identifier)
        if (currentDoc && !currentDoc.entry['deleted'] && currentDoc.checksum != newDoc.checksum) {
            newDoc.setVersion(currentDoc.version + 1)
        } else {
            currentDoc = null
        }
        return [currentDoc, newDoc]
    }

    protected prepareIndexingRequest(doc, identifier, idxName) {
        if (identifier) {
            String encodedIdentifier = toElasticId(doc.identifier)
            return client.prepareIndex(idxName, ELASTIC_STORAGE_TYPE, encodedIdentifier).setSource(doc.toJson().getBytes("UTF-8"))
        } else {
            return client.prepareIndex(idxName + VERSION_STORAGE_SUFFIX, ELASTIC_STORAGE_TYPE).setSource(doc.toJson().getBytes("UTF-8"))
        }
    }

    Document get(String identifier) {
        return get(identifier, null)
    }

    @Override
    Document get(String identifier, String version) {
        def grq = client.prepareGet(indexName, ELASTIC_STORAGE_TYPE, toElasticId(identifier))
        def response = grq.execute().actionGet();

        int v = (version ? version as int : -1)

        Document document = null

        if (response.exists) {
            log.trace("Get response for ${identifier}: " + response.sourceAsMap)
            document = whelk.createDocumentFromJson(response.sourceAsString)
        }

        if (document && (v < 0 || document.version == v)) {
            return document
        } else if (document != null) {
            log.debug("Current version (${document.version}) of document not the one requested ($v). Looking in the cellar ...")
            def query = boolQuery().must(termQuery("identifier", document.identifier)).must(termQuery("entry.version", v))
            def srq = client.prepareSearch(indexName + VERSION_STORAGE_SUFFIX).setTypes([ELASTIC_STORAGE_TYPE] as String[]).setQuery(query).setSize(1)
            response = performExecute(srq)
            log.trace("Response from version search: $response")
            if (response.hits.totalHits == 1) {
                return whelk.createDocumentFromJson(response.hits.hits[0].sourceAsString)
            }
        }
        return null
    }

    Document getByAlternateIdentifier(String identifier) {
        def query = termQuery("entry.alternateIdentifiers", identifier)
        def srq = client.prepareSearch(indexName).setTypes([ELASTIC_STORAGE_TYPE] as String[]).setQuery(query).setSize(1)
        def response = performExecute(srq)
        log.trace("Response from alternate identifiers search: $response")
        if (response.hits.totalHits == 1) {
            return whelk.createDocumentFromJson(response.hits.hits[0].sourceAsString)
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
    void remove(String identifier) {
        if (!versioning) {
            log.debug("Deleting record at $indexName with id ${toElasticId(identifier)}")
            client.delete(new DeleteRequest(indexName, ELASTIC_STORAGE_TYPE, toElasticId(identifier)))
        } else {
            log.debug("Creating tombstone record at $indexName with id ${toElasticId(identifier)}")
            store(createTombstone(identifier))
        }
    }

    private Document createTombstone(id) {
        def tombstone = whelk.createDocument("text/plain").withIdentifier(id).withData("DELETED ENTRY")
        tombstone.entry['deleted'] = true
        return tombstone
    }
}
