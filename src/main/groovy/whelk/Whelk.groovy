package whelk

import groovy.util.logging.Slf4j as Log

import whelk.component.Index
import whelk.component.PostgreSQLComponent

/**
 * Created by markus on 15-09-03.
 */
@Log
class Whelk {

    PostgreSQLComponent storage
    Index elastic

    public Whelk(PostgreSQLComponent pg, Index es) {
        this.storage = pg
        this.elastic = es
        log.info("Whelk started")
    }

    Document store(Document document) {
        if (storage.store(document)) {
            elastic.index(document)
        }
        return document
    }

    void bulkStore(List<Document> documents, String dataset) {
        if (storage.bulkStore(documents, dataset)) {
            elastic.bulkIndex(documents)
        }
    }

    void remove(String id, String dataset) {
        if (storage.remove(id, dataset)) {
            elastic.remove(id, dataset)
        }
    }
}
