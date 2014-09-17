package se.kb.libris.whelks.component.support
import groovy.util.logging.Slf4j as Log

import org.elasticsearch.common.settings.*
import org.elasticsearch.node.NodeBuilder
import static org.elasticsearch.node.NodeBuilder.*

import se.kb.libris.whelks.plugin.*

@Log
class ElasticSearchNode extends BasicPlugin {

    ElasticSearchNode() {
        this(null)
    }

    ElasticSearchNode(String dataDir) {
        log.info "Starting elastic node"
        def elasticcluster = System.getProperty("elastic.cluster")
        ImmutableSettings.Builder sb = ImmutableSettings.settingsBuilder()
        sb.put("node.name", "Parasite")
        if (elasticcluster) {
            sb = sb.put("cluster.name", elasticcluster)
        } else {
            sb = sb.put("cluster.name", "bundled_whelk_index")
        }
        if (dataDir != null) {
            sb.put("path.data", dataDir)
        } else {
            sb.put("path.data", "work/data")
        }
        sb.build()
        Settings settings = sb.build()
        NodeBuilder nBuilder = nodeBuilder().settings(settings)
        // start it!
        def node = nBuilder.build().start()
        log.info("Elasticsearch node started.")
    }
}

