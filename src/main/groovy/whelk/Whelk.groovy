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

    void store(Document document) {
        if (storage.store(document)) {
            elastic.index(document.id, document.dataset, document.data)
        }
    }

    void bulkStore(List<Document> documents) {
        if (storage.bulkStore(documents, documents.first().dataset)) {
            //elastic.bulkIndex(documents)
        }
    }
}
