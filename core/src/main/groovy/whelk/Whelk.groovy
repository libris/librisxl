package whelk

import groovy.util.logging.Slf4j as Log

import whelk.component.Index
import whelk.component.Storage
import whelk.filter.JsonLdLinkExpander

/**
 * Created by markus on 15-09-03.
 */
@Log
class Whelk {

    Storage storage
    Index elastic
    JsonLdLinkExpander expander

    public Whelk(Storage pg, Index es, JsonLdLinkExpander ex) {
        this.storage = pg
        this.elastic = es
        this.expander = ex
        log.info("Whelk started with storage ${storage}, index $elastic and expander.")
    }

    public Whelk(Storage pg, Index es) {
        this.storage = pg
        this.elastic = es
        log.info("Whelk started with storage $storage and index $elastic")
    }

    public Whelk(Storage pg) {
        this.storage = pg
        log.info("Whelk started with storage $storage")
    }

    public Whelk() {
    }

    Document store(Document document) {
        if (storage.store(document) && elastic) {
            elastic.index(document)
        }
        return document
    }

    void bulkStore(List<Document> documents) {
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
