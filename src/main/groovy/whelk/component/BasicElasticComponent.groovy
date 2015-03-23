package whelk.component

import org.apache.commons.codec.binary.Base64

import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.*
import org.elasticsearch.common.transport.*
import org.elasticsearch.common.settings.*
import org.elasticsearch.action.delete.*
import org.elasticsearch.action.admin.indices.flush.*
import org.elasticsearch.action.admin.indices.alias.get.*

import whelk.exception.*

abstract class BasicElasticComponent extends BasicComponent implements ShapeComputer {
    Client client
    def defaultMapping, es_settings
    static final String METAENTRY_INDEX_TYPE = "entry"
    static final String DEFAULT_CLUSTER = "whelks"
    int WARN_AFTER_TRIES = 1000
    int RETRY_TIMEOUT = 300
    int MAX_RETRY_TIMEOUT = 60*60*1000
    static int MAX_NUMBER_OF_FACETS = 100
    String elastichost, elasticcluster
    int elasticport = 9300

    int batchUpdateSize = 2000

    String defaultType
    static final String DEFAULT_TYPE = "record"

    BasicElasticComponent() {
        super()
        connectClient()
    }
    BasicElasticComponent(Map settings) {
        super()
        this.elastichost = settings.get('elasticHost')
        if (!elastichost) {
            this.elastichost = System.getProperty("elastic.host")
        }
        this.elasticcluster = settings.get('elasticCluster')
        if (!elasticcluster) {
            this.elasticcluster = System.getProperty("elastic.cluster", DEFAULT_CLUSTER)
        }
        this.elasticport = settings.get('elasticPort', System.getProperty("elastic.port", ""+elasticport)) as int
        this.batchUpdateSize = settings.get('batchUpdateSize', batchUpdateSize)
        this.defaultType = settings.get("defaultType", DEFAULT_TYPE)
        connectClient()
    }

    void connectClient() {
        if (elastichost) {
            log.info("Connecting to $elastichost:$elasticport using cluster $elasticcluster")
            def sb = ImmutableSettings.settingsBuilder()
                .put("client.transport.ping_timeout", 30000)
                .put("client.transport.sniff", true)
            if (elasticcluster) {
                sb = sb.put("cluster.name", elasticcluster)
            }
            Settings elasticSettings = sb.build();
            client = new TransportClient(elasticSettings).addTransportAddress(new InetSocketTransportAddress(elastichost, elasticport))
            log.debug("... connected")
        } else {
            throw new WhelkRuntimeException("Unable to initialize ${this.id}. Need to configure plugins.json or set system property \"elastic.host\" and possibly \"elastic.cluster\".")
        }
    }

    def performExecute(def requestBuilder) {
        int failcount = 0
        def response = null
        while (response == null) {
            try {
                response = requestBuilder.execute().actionGet()
            } catch (NoNodeAvailableException n) {
                log.trace("Retrying server connection ...")
                if (failcount++ > WARN_AFTER_TRIES) {
                    log.warn("Failed to connect to elasticsearch after $failcount attempts.")
                }
                if (failcount % 100 == 0) {
                    log.info("Server is not responsive. Still trying ...")
                }
                Thread.sleep(RETRY_TIMEOUT + failcount > MAX_RETRY_TIMEOUT ? MAX_RETRY_TIMEOUT : RETRY_TIMEOUT + failcount)
            }
        }
        return response
    }

    void createIndexIfNotExists(String indexName, boolean storageIndex = false) {
        if (!performExecute(client.admin().indices().prepareExists(indexName)).exists) {
            log.info("Couldn't find index by name $indexName. Creating ...")
            if (indexName.startsWith(".") || storageIndex) {
                // It's a meta/storage index. No need for aliases and such.
                if (!es_settings) {
                    es_settings = loadJson("es_settings.json")
                }
                performExecute(client.admin().indices().prepareCreate(indexName).setSettings(es_settings))
            } else {
                String currentIndex = createNewCurrentIndex(indexName)
                log.debug("Will create alias $indexName -> $currentIndex")
                performExecute(client.admin().indices().prepareAliases().addAlias(currentIndex, indexName))
            }
        } else if (getRealIndexFor(indexName) == null) {
            throw new WhelkRuntimeException("Unable to find a real current index for $indexName")
        }
    }

    String getRealIndexFor(String alias) {
        def aliases = performExecute(client.admin().cluster().prepareState()).state.metaData.aliases()
        log.trace("aliases: $aliases")
        def ri = null
        if (aliases.containsKey(alias)) {
            ri = aliases.get(alias)?.keys().iterator().next()
        }
        if (ri) {
            log.trace("ri: ${ri.value} (${ri.value.getClass().getName()})")
        }
        return (ri ? ri.value : alias)
    }

    void flush() {
        log.debug("Flusing ${this.id}")
        def flushresponse = performExecute(new FlushRequestBuilder(client.admin().indices()))
    }

    /*
    void index(final List<Map<String,String>> data) throws WhelkIndexException  {
        def breq = client.prepareBulk()
        for (entry in data) {
            breq.add(client.prepareIndex(entry['index'], entry['type'], entry['id']).setSource(entry['data'].getBytes("utf-8")))
        }
        def response = performExecute(breq)
        if (response.hasFailures()) {
            log.error "Bulk entry indexing has failures."
            def fails = []
            for (re in response.items) {
                if (re.failed) {
                    log.error "Fail message for id ${re.id}, type: ${re.type}, index: ${re.index}: ${re.failureMessage}"
                    try {
                        fails << fromElasticId(re.id)
                    } catch (Exception e1) {
                        log.error("TranslateIndexIdTo cast an exception", e1)
                        fails << "Failed translation for \"$re\""
                    }
                }
            }
            throw new WhelkIndexException("Failed to index entries. Reason: ${response.buildFailureMessage()}", new WhelkAddException(fails))
        } else {
            log.debug("Direct bulk request completed in ${response.tookInMillis} millseconds.")
        }
    }

    void index(byte[] data, Map params) throws WhelkIndexException  {
        try {
            def response = performExecute(client.prepareIndex(params['index'], params['type'], params['id']).setSource(data))
            log.debug("Raw byte indexer (${params.index}/${params.type}/${params.id}) indexed version: ${response.version}")
        } catch (Exception e) {
            throw new WhelkIndexException("Failed to index ${new String(data)} with params $params", e)
        }
    }

    void deleteEntry(String identifier, indexName, indexType) {
        def response = performExecute(client.prepareDelete(indexName, indexType, toElasticId(identifier)))
        log.debug("Deleted ${response.id} with type ${response.type} from ${response.index}. Document found: ${response.found}")
    }
    */

    @Deprecated
    void checkTypeMapping(indexName, indexType) {
        log.debug("Checking mappings for index $indexName, type $indexType")
        def mappings = performExecute(client.admin().cluster().prepareState()).state.metaData.index(indexName).getMappings()
        log.trace("Mappings: $mappings")
        if (!mappings.containsKey(indexType)) {
            log.debug("Mapping for $indexName/$indexType does not exist. Creating ...")
            setTypeMapping(indexName, indexType)
        }
    }

    @Deprecated
    protected void setTypeMapping(indexName, itype) {
        log.info("Creating mappings for $indexName/$itype ...")
        //XContentBuilder mapping = jsonBuilder().startObject().startObject("mappings")
        if (!defaultMapping) {
            defaultMapping = loadJson("default_mapping.json")
        }
        def typePropertyMapping = loadJson("${itype}_mapping_properties.json")
        def typeMapping
        if (typePropertyMapping) {
            log.debug("Found properties mapping for $itype. Using them with defaults.")
            typeMapping = new HashMap(defaultMapping)
            typeMapping.put("properties", typePropertyMapping.get("properties"))
        } else {
            typeMapping = loadJson("${itype}_mapping.json") ?: defaultMapping
        }
        // Append special mapping for @id-fields
        if (!typeMapping.dynamic_templates) {
            typeMapping['dynamic_templates'] = []
        }
        if (!typeMapping.dynamic_templates.find { it.containsKey("id_template") }) {
            log.debug("Found no id_template. Creating.")
            typeMapping.dynamic_templates << ["id_template":["match":"@id","match_mapping_type":"string","mapping":["type":"string","index":"not_analyzed"]]]
        }

        String mapping = mapper.writeValueAsString(typeMapping)
        log.debug("mapping for $indexName/$itype: " + mapping)
        def response = performExecute(client.admin().indices().preparePutMapping(indexName).setType(itype).setSource(mapping))
        log.debug("mapping response: ${response.acknowledged}")
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

    /**
     * ShapeComputer methods
     */

    String calculateTypeFromIdentifier(String id) {
        String identifier = new URI(id).path.toString()
        log.debug("Received uri $identifier")
        String idxType
        try {
            def identParts = identifier.split("/")
            idxType = (identParts[1] == whelk.id && identParts.size() > 3 ? identParts[2] : identParts[1])
        } catch (Exception e) {
            log.error("Tried to use first part of URI ${identifier} as type. Failed: ${e.message}")
        }
        if (!idxType) {
            idxType = defaultType
        }
        log.debug("Using type $idxType for ${identifier}")
        return idxType
    }

    String toElasticId(String id) {
        return Base64.encodeBase64URLSafeString(id.getBytes("UTF-8"))
    }

    String fromElasticId(String id) {
        if (id.contains("::")) {
            log.warn("Using old style index id's for $id")
            def pathelements = []
            id.split("::").each {
                pathelements << java.net.URLEncoder.encode(it, "UTF-8")
            }
            return  new String("/"+pathelements.join("/"))
        } else {
            String decodedIdentifier = new String(Base64.decodeBase64(id), "UTF-8")
            log.debug("Decoded new style id into $decodedIdentifier")
            return decodedIdentifier
        }
    }

}
