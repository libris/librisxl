package whelk

import groovy.util.logging.Slf4j as Log

import whelk.component.Index
import whelk.component.PostgreSQLComponent
import whelk.filter.JsonLdLinkExpander

/**
 * Created by markus on 15-09-03.
 */
@Log
class Whelk {

    PostgreSQLComponent storage
    Index elastic
    JsonLdLinkExpander expander

    public Whelk(PostgreSQLComponent pg, Index es, JsonLdLinkExpander ex) {
        this.storage = pg
        this.elastic = es
        this.expander = ex
        log.info("Whelk started")
    }

    public Whelk(PostgreSQLComponent pg, Index es) {
        this.storage = pg
        this.elastic = es
        log.info("Whelk started")
    }

    public Whelk(PostgreSQLComponent pg) {
        this.storage = pg
    }

    Document store(Document document) {
        if (storage.store(document) && elastic) {
            elastic.index(document)
        }
        return document
    }

    void bulkStore(List<Document> documents, String dataset) {
        if (storage.bulkStore(documents) && elastic) {
            elastic.bulkIndex(documents)
        }
    }

    void remove(String id, String dataset) {
        if (storage.remove(id, dataset) && elastic) {
            elastic.remove(id, dataset)
        }
    }
}
