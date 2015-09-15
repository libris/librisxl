package whelk

import groovy.util.logging.Slf4j as Log

import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent

/**
 * Created by markus on 15-09-03.
 */
@Log
class Whelk {

    PostgreSQLComponent storage
    ElasticSearch elastic

    public Whelk(PostgreSQLComponent pg, ElasticSearch es) {
        this.storage = pg
        this.elastic = es
        log.info("Whelk started")
    }

    String store(Document document) {
        if (storage.store(document)) {
            elastic.index(document.id, document.dataset, document.data)
        }
        return document.identifier
    }

    void bulkStore(List<Document> documents, String dataset) {
        if (storage.bulkStore(documents, dataset)) {
            //elastic.bulkIndex(documents)
        }
    }

    void remove(String id, String dataset) {}
}
