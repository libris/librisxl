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
class ElasticSearchStorage extends ElasticSearch implements Storage {

    final static String ELASTIC_STORAGE_TYPE = "document"
    boolean versioning
    boolean readOnly = false

    int batchUpdateSize = 2000

    def es_settings, defaultMapping
    String indexName

    ElasticSearchStorage(String componentId = null, Map settings) {
        super(settings)
        this.contentTypes = settings.get('contentTypes', null)
        this.indexName = settings.get('indexName', null)
        this.versioning = settings.get('versioning', false)
        this.readOnly = settings.get('readOnly', false)
        id = componentId
    }

    void componentBootstrap(String str) {
        super.componentBootstrap(str)
        if (!this.indexName) {
            this.indexName = str+"_"+this.id
        }
        log.info("Elastic Storage using index name $indexName")
    }

    @Override
    void onStart() {
        log.info("Starting ${this.id} with index name $indexName")
        createIndexIfNotExists(indexName)
        checkTypeMapping(indexName, ELASTIC_STORAGE_TYPE)
        if (versioning) {
            createIndexIfNotExists(indexName + VERSION_STORAGE_SUFFIX)
            checkTypeMapping(indexName + VERSION_STORAGE_SUFFIX, ELASTIC_STORAGE_TYPE)
        }
    }


    @Override
    boolean store(Document doc, boolean withVersioning = versioning) {
        if (readOnly) {
            log.debug("Storage is read only. Not saving.")
            return true
        }
        try {
            if (versioning && withVersioning) {
                performExecute(prepareIndexingRequest(doc, null, indexName))
            }
            performExecute(prepareIndexingRequest(doc, doc.identifier, indexName))
            return true
        } catch (Exception e) {
            log.error("Failed to save document ${doc?.identifier}", e)
        }
        return false
    }

    @Override
    void bulkStore(final List docs, String dataset) {
        if (readOnly) {
            log.debug("Storage is read only. Not saving.")
            return
        }
        log.debug("Bulk store requested. Versioning set to $versioning")
        def breq = client.prepareBulk()
        for (doc in docs) {
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
            int v = 0
            response.hits.hits.each {
                def doc = whelk.createDocumentFromJson(it.sourceAsString)
                doc.version = v++
                docs << doc
            }
            return docs
        } else {
            return [load(identifier)]
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
        if (identifier) {
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
                                        def timeRangeQuery = rangeQuery("entry.modified")
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
        if (readOnly) {
            log.debug("Storage is read only. Not removing.")
            return
        }
        if (!versioning) {
            log.debug("Deleting record at $indexName with id ${toElasticId(identifier)}")
            client.delete(new DeleteRequest(indexName, ELASTIC_STORAGE_TYPE, toElasticId(identifier)))
        } else {
            log.debug("Creating tombstone record at $indexName with id ${toElasticId(identifier)}")
            store(createTombstone(identifier, dataset))
        }
    }


    // Elastic maintenance methods
    void createIndexIfNotExists(String indexName) {
        if (!performExecute(client.admin().indices().prepareExists(indexName)).exists) {
            log.info("Couldn't find index by name $indexName. Creating ...")
            /*
            if (!es_settings) {
                es_settings = loadJson("es_settings.json")
            }
            */
            performExecute(client.admin().indices().prepareCreate(indexName)) // .setSettings(es_settings))
        }
    }

    def loadJson(String file) {
        log.trace("Loading file $file")
        def json
        String basefile = file
        if (!getClass().classLoader.findResource(file)) {
            file = "es/" + basefile
        }
        if (!getClass().classLoader.findResource(file)) {
            file = "es/" + whelk.props.get("ES_CONFIG_PREFIX") + "/" + basefile
        }
        try {
            json = getClass().classLoader.getResourceAsStream(file).withStream {
                mapper.readValue(it, Map)
            }
        } catch (NullPointerException npe) {
            log.trace("File $file not found.")
        }
        return json
    }

    void checkTypeMapping(indexName, indexType) {
        log.debug("Checking mappings for index $indexName, type $indexType")
        def mappings = performExecute(client.admin().cluster().prepareState()).state.metaData.index(indexName).getMappings()
        log.trace("Mappings: $mappings")
        if (!mappings.containsKey(indexType)) {
            log.debug("Mapping for $indexName/$indexType does not exist. Creating ...")
            setTypeMapping(indexName, indexType)
        }
    }

    protected void setTypeMapping(indexName, itype) {
        log.info("Creating mappings for $indexName/$itype ...")
        def documentTypeMapping = [
            "_timestamp" : [
                "enabled" : true,
                "store" : true,
                "path" : "entry.modified"
            ],
            "_source" : [
                "enabled" : true
            ],
            "properties" : [
                "data": [
                    "type": "binary"
                ],
                "entry" : [
                    "properties" : [
                        "identifier" : [
                            "type" : "string",
                            "index" : "not_analyzed"
                        ],
                        "checksum" : [
                            "type" : "string",
                            "index" : "not_analyzed"
                        ],
                        "dataset" : [
                            "type" : "string",
                            "index" : "not_analyzed"
                        ],
                        "created" : [
                            "type" : "date"
                        ],
                        "modified" : [
                            "type" : "date"
                        ],
                        "contentType" : [
                            "type" : "string",
                            "index" : "not_analyzed"
                        ],
                        "alternateIdentifiers" : [
                            "type" : "string",
                            "index" : "not_analyzed"
                        ],
                        "alternateDatasets" : [
                            "type" : "string",
                            "index" : "not_analyzed"
                        ]
                    ]
                ]
            ]
        ]

        String mapping = mapper.writeValueAsString(documentTypeMapping)
        log.debug("mapping for $indexName/$itype: " + mapping)
        def response = performExecute(client.admin().indices().preparePutMapping(indexName).setType(itype).setSource(mapping))
        log.debug("mapping response: ${response.acknowledged}")
    }


}
