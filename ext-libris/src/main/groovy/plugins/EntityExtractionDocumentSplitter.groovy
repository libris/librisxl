package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log
import se.kb.libris.whelks.*

@Log
class EntityExtractionDocumentSplitter extends BasicPlugin implements DocumentSplitter {
    String requiredContentType = "application/json"

    List<Document> split(Document doc) {
        List<Document> docs = []
        if (handles(doc)) {
            for (docEntity in doc.dataAsMap.get("extracted_entities")) {
                docs << new Document()
                    .withData(docEntity.get('entity'))
                    .withEntry(['dataset':docEntity['dataset']])
                    .withContentType(docEntity['contentType'])
                    .withIdentifier(docEntity['id'])
            }
            log.debug("Returning list of documents with ${docs.size()} entries.")
            return docs
        }
        return [doc]
    }

    boolean handles(Document doc) {
        if (doc && doc.isJson()) {
            return doc.dataAsMap.containsKey("extracted_entities")
        }
        return false
    }
}
