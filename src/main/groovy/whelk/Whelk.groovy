package whelk

import groovy.util.logging.Slf4j as Log
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
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

    Document store(Document document) {
        if (storage.store(document)) {
            def idxReq = new IndexRequest(elastic.getIndexName(), document.dataset, elastic.toElasticId(document.id)).source(document.data)
            def response = elastic.performExecute(idxReq)
            log.debug("Indexed the document ${document.id} as ${elastic.indexName}/${document.dataset}/${response.getId()} as version ${response.getVersion()}")
        }
        return document
    }

    void bulkStore(List<Document> documents, String dataset) {
        if (storage.bulkStore(documents, dataset)) {
            BulkRequest bulk = new BulkRequest()
            for (doc in documents) {
                bulk.add(new IndexRequest(elastic.getIndexName(), doc.dataset, elastic.toElasticId(doc.id)))
            }
            BulkResponse response = elastic.performExecute(bulk)
            if (response.hasFailures()) {
                response.iterator().each {
                    if (it.failed) {
                        log.error("Indexing of ${it.id} (${elastic.fromElasticId(it.id)}) failed: ${it.failureMessage}")
                    }
                }
            }
        }
    }

    void remove(String id, String dataset) {}
}
