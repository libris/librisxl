package whelk

import groovy.util.logging.Slf4j as Log
import org.picocontainer.Characteristics
import org.picocontainer.DefaultPicoContainer
import org.picocontainer.containers.PropertiesPicoContainer
import whelk.component.Index
import whelk.component.PostgreSQLComponent
import whelk.filter.JsonLdLinkExpander
import whelk.util.LegacyIntegrationTools
import whelk.util.PropertyLoader

/**
 * Created by markus on 15-09-03.
 */
@Log
class Whelk {

    PostgreSQLComponent storage
    Index elastic
    JsonLdLinkExpander expander
    Map displayData
    Map vocabData
    JsonLd jsonld

    String vocabDisplayUri = "https://id.kb.se/vocab/display" // TODO: encapsulate and configure (LXL-260)
    String vocabUri = "https://id.kb.se/vocab/" // TODO: encapsulate and configure (LXL-260)

    public Whelk(PostgreSQLComponent pg, Index es, JsonLdLinkExpander ex) {
        this.storage = pg
        this.elastic = es
        this.expander = ex
        log.info("Whelk started with storage ${storage}, index $elastic and expander.")
    }

    public Whelk(PostgreSQLComponent pg, Index es) {
        this.storage = pg
        this.elastic = es
        log.info("Whelk started with storage $storage and index $elastic")
    }

    public Whelk(PostgreSQLComponent pg) {
        this.storage = pg
        log.info("Whelk started with storage $storage")
    }

    public Whelk() {
    }

    public static DefaultPicoContainer getPreparedComponentsContainer(Properties properties) {
        DefaultPicoContainer pico = new DefaultPicoContainer(new PropertiesPicoContainer(properties))
        Properties componentProperties = PropertyLoader.loadProperties("component")
        for (comProp in componentProperties) {
            if (comProp.key.endsWith("Class") && comProp.value && comProp.value != "null") {
                log.info("Adding pico component ${comProp.key} = ${comProp.value}")
                pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(Class.forName(comProp.value))
            }
        }
        pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(Whelk.class)
        return pico
    }

    void loadCoreData() {
        loadDisplayData()
        loadVocabData()
        jsonld = new JsonLd(displayData, vocabData)
    }

    void loadDisplayData() {
        this.displayData = this.storage.locate(vocabDisplayUri, true).document.data
    }

    void loadVocabData() {
        this.vocabData = this.storage.locate(vocabUri, true).document.data
    }

    Map<String, Document> bulkLoad(List ids) {
        Map result = [:]
        ids.each { id ->
            Document doc
            if (id.startsWith(Document.BASE_URI.toString())) {
                id = Document.BASE_URI.resolve(id).getPath().substring(1)
                doc = storage.load(id)
            } else {
                doc = storage.locate(id, true)?.document
            }

            if (doc && !doc.deleted) {
                result[id] = doc
            }
        }
        return result
    }

    private void reindexDependers(Document document) {
        List<String> dependingIDs = storage.getDependers(document.getId())
        Map dependingDocuments = bulkLoad(dependingIDs)
        for (String id : dependingDocuments.keySet()) {
            Document dependingDoc = dependingDocuments.get(id)
            String dependingDocCollection = LegacyIntegrationTools.determineLegacyCollection(dependingDoc, jsonld)
            elastic.index(dependingDoc, dependingDocCollection)
        }
    }

    /**
     * NEVER use this to _update_ a document. Use storeAtomicUpdate() instead. Using this for new documents is fine.
     */
    Document store(Document document, String changedIn, String changedBy, String collection, boolean deleted, boolean createOrUpdate = true) {
        if (storage.store(document, createOrUpdate, changedIn, changedBy, collection, deleted)) {
            if (elastic) {
                elastic.index(document, collection)
                reindexDependers(document)
            }
        }
        return document
    }

    Document storeAtomicUpdate(String id, boolean minorUpdate, String changedIn, String changedBy, String collection, boolean deleted, PostgreSQLComponent.UpdateAgent updateAgent) {
        Document updated = storage.storeAtomicUpdate(id, minorUpdate, changedIn, changedBy, collection, deleted, updateAgent)
        if (elastic) {
            elastic.index(updated, collection)
            reindexDependers(updated)
        }
        return updated
    }

    void bulkStore(final List<Document> documents, String changedIn, String changedBy, String collection, boolean createOrUpdate = true) {
        if (storage.bulkStore(documents, createOrUpdate, changedIn, changedBy, collection)) {
            if (elastic) {
                elastic.bulkIndex(documents, collection)
                for (Document doc : documents) {
                    reindexDependers(doc)
                }
            }
        } else {
            log.warn("Bulk store failed, not indexing : ${documents.first().id} - ${documents.last().id}")
        }
    }

    void remove(String id, String changedIn, String changedBy, String collection) {
        log.debug "Deleting ${id} from Whelk"
        if (storage.remove(id, changedIn, changedBy, collection)) {
            if (elastic) {
                elastic.remove(id)
                log.debug "Object ${id} was removed from Whelk"
            }
            else {
                log.warn "No Elastic present when deleting. Skipping call to elastic.remove(${id})"
            }
        } else {
            log.warn "storage did not remove ${id} from whelk"
        }
    }
}
