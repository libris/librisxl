package whelk.component.support
import groovy.util.logging.Slf4j as Log

import org.elasticsearch.common.settings.*
import org.elasticsearch.node.NodeBuilder
import static org.elasticsearch.node.NodeBuilder.*

import whelk.plugin.*

@Log
class ElasticSearchNode extends BasicPlugin {

    ElasticSearchNode(String dataDir, String indexName) {
        log.info "Starting elastic node"
        def elasticcluster = System.getProperty("elastic.cluster")
        ImmutableSettings.Builder sb = ImmutableSettings.settingsBuilder()
        sb.put("node.name", "Parasite")
        if (elasticcluster) {
            sb = sb.put("cluster.name", elasticcluster)
        } else {
            sb = sb.put("cluster.name", "bundled_whelk_index")
        }
        sb.put("path.data", dataDir)
        Settings settings = sb.build()
        NodeBuilder nBuilder = nodeBuilder().settings(settings)
        // start it!
        def node = nBuilder.build().start()
        log.info("Elasticsearch node started.")
        if (!node.client().admin().indices().prepareExists(indexName).execute().actionGet().exists) {
            log.info("Creating local index $indexName")
            def config = mapper.readValue(new FileInputStream("librisxl-tools/elasticsearch/config_libris.json"), Map)
            node.client().admin().indices().prepareCreate(indexName).setSettings(config.get("settings", [:])).execute().actionGet()
            def defaultMappings = config.mappings['_default_']
            config.mappings.each {
                if (it.key != '_default_') {
                    def typeMapping = new HashMap(defaultMappings)
                    typeMapping.put("properties", it.value.properties)
                    String mapping = mapper.writeValueAsString(typeMapping)
                    log.info("Applying mappings for ${it.key} ... ")
                    def response = node.client().admin().indices().preparePutMapping(indexName).setType(it.key).setSource(mapping).execute().actionGet()
                    if (!response.acknowledged) {
                        log.warn("Failed to set mappings for ${indexName}/${it.key}")
                    }
                }
            }
        }
        log.info("Elasticsearch node ready.")
    }
}

