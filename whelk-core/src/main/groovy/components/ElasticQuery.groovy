package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.exception.WhelkRuntimeException

@Log
class ElasticQuery extends Query {
    String indexType

    ElasticQuery() {super()}

    ElasticQuery(String qs) {
        super(qs)
    }
    ElasticQuery(Map qmap) {
        super(qmap)
        if (qmap.get("type")) {
            this.indexType = qmap.get("type")
        }
        if (qmap.get("indexType")) {
            this.indexType = qmap.get("indexType")
        }
    }

    ElasticQuery(Query q) {
        q.properties.each { name, value ->
            log.trace("[ElasticQuery] setting $name : $value")
            try {
                this."$name" = value
            } catch (groovy.lang.ReadOnlyPropertyException rope) {
                log.trace("[ElasticQuery] ${rope.message}")
            }
        }
    }
}
