package whelk

import groovy.util.logging.Slf4j as Log
import whelk.component.APIX
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
    APIX apix
    JsonLdLinkExpander expander

    public Whelk(Storage pg, Index es, APIX a, JsonLdLinkExpander ex) {
        this.storage = pg
        this.elastic = es
        this.apix = a
        this.expander = ex
        log.info("Whelk started with storage ${storage}, index $elastic, apix $apix and expander.")
    }

    public Whelk(Storage pg, Index es, JsonLdLinkExpander ex) {
        this.storage = pg
        this.elastic = es
        this.expander = ex
        log.info("Whelk started with storage ${storage}, index $elastic and expander.")
    }

    public Whelk(Storage pg, Index es, APIX a) {
        this.storage = pg
        this.elastic = es
        this.apix = a
        log.info("Whelk started with storage $storage and index $elastic and apix $apix ")
    }

    public Whelk(Storage pg, Index es) {
        this.storage = pg
        this.elastic = es
        log.info("Whelk started with storage $storage and index $elastic")
    }

    public Whelk(Storage pg, APIX a) {
        this.storage = pg
        this.apix = a
        log.info("Whelk started with storage $storage and apix $apix")
    }


    public Whelk(Storage pg) {
        this.storage = pg
        log.info("Whelk started with storage $storage")
    }

    public Whelk() {
    }

    Document store(Document document) {
        if (storage.store(document)) {
            if (elastic) {
                elastic.index(document)
            }
            if (apix) {
                apix.send(document)
            }
        }
        return document
    }

    void bulkStore(List<Document> documents) {
        if (storage.bulkStore(documents)) {
            if (elastic) {
                elastic.bulkIndex(documents)
            }
        } else {
            log.warn("Bulk store failed, not indexing : ${documents.first().id} - ${documents.last().id}")
        }
    }

    void remove(String id, String dataset) {
        if (storage.remove(id, dataset) && elastic) {
            elastic.remove(id, dataset)
        }
    }
}
