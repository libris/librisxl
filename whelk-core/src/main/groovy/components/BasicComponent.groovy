package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper

import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.*

@Log
abstract class BasicComponent extends BasicPlugin implements Component {

    static final ObjectMapper mapper = new ObjectMapper()
    static final String LAST_UPDATED = "last_updated"

    static final String STATE_FILE_SUFFIX = ".state"

    private Map componentState
    boolean stateUpdated

    Whelk whelk
    List contentTypes
    Map<String,FormatConverter> formatConverters = new HashMap<String,FormatConverter>()
    Map<String,DocumentSplitter> documentSplitters = new HashMap<String,DocumentSplitter>()

    void init(String whelkId) {
        File stateFile= new File("${global.WHELK_WORK_DIR}/${whelkId}_${this.id}${STATE_FILE_SUFFIX}")
        if (stateFile.exists()) {
            log.info("Loading persisted state for [${this.id}].")
            try {
                componentState = mapper.readValue(stateFile, Map).asSynchronized()
            } catch (Exception e) {
                log.error("[${this.id}] Failed to read statefile: ${e.message}", e)
                throw new WhelkRuntimeException("Couldn't read statefile for ${whelkId}/${this.id}", e)
            }
        } else {
            componentState = [:].asSynchronized()
            componentState['lastUpdated'] = 0L
        }
        log.info("[${this.id}] Starting thread to periodically save state.")
        Thread.start {
            boolean ok = true
            while(ok) {
                try {
                    if (componentState && stateUpdated) {
                        log.trace("[${whelkId}-${this.id}] Saving state ...")
                        mapper.writeValue(stateFile, componentState)
                        stateUpdated = false
                    }
                    Thread.sleep(20000)
                } catch (Exception e) {
                    ok = false
                    log.error("[${this.id}] Failed to write statefile: ${e.message}", e)
                    throw new WhelkRuntimeException("Couldn't write statefile for ${whelkId}/${this.id}", e)
                }
            }
        }
        componentState['componentId'] = this.id
        componentState['status'] = "started"

        log.debug("[${this.id}] Loading format converters")
        for (f in plugins.findAll { it instanceof FormatConverter }) {
            formatConverters.put(f.requiredContentType, f)
        }
        log.debug("[${this.id}] Loading document splitters")
        for (d in plugins.findAll { it instanceof DocumentSplitter }) {
            documentSplitters.put(d.requiredContentType, d)
        }
    }

    @Override
    public URI add(Document document) {
        try {
            List<Document> docs = prepareDocs([document], document.contentType)
            batchLoad(docs)
            setState(LAST_UPDATED, new Date().getTime())
            return new URI(document.identifier)
        } catch (Exception e) {
            log.error("[${this.id}] failed to add documents. (${e.message})", e)
            throw e
        }
    }

    List<Document> prepareDocs(final List<Document> documents, String contentType) {
        FormatConverter fc = formatConverters.get(contentType)
        DocumentSplitter preSplitter = documentSplitters.get(contentType)

        DocumentSplitter postSplitter = documentSplitters.get(contentType)
        log.trace("fc: $fc")
        log.trace("postSplitter: $postSplitter")
        if (!postSplitter && fc) {
            postSplitter = documentSplitters.get(fc.resultContentType)
        }
        if (fc || preSplitter || postSplitter) {
            List docs = []
            for (doc in documents) {
                log.trace("[${this.id}] Calling prepare on doc ${doc.identifier}");
                if (preSplitter) {
                    log.trace("[${this.id}] Running preSplitter")
                    for (d in preSplitter.split(doc)) {
                        if (fc) {
                            log.trace(" ... with conversion")
                            docs << fc.convert(d)
                        } else {
                            log.trace(" ... without conversion")
                            docs << d
                        }
                    }
                } else if (fc) {
                    log.trace("[${this.id}] Adding document after conversion.")
                    docs.add(fc.convert(doc))
                } else {
                    log.trace("[${this.id}] Adding document without conversion.")
                    docs.add(doc)
                }
                if (postSplitter) {
                    log.trace("[${this.id}] Running postSplitter")
                    if (docs.size() > 0) {
                        List<Document> convertedDocs = []
                        for (d in docs) {
                            convertedDocs.addAll(postSplitter.split(d))
                        }
                        docs = convertedDocs
                    }
                }
            }
            log.debug("[${this.id}] Returning document list of size ${docs.size()}.")
            return docs
        }
        log.debug("[${this.id}] Nothing to do, return the same list of documents.")
        return documents
    }

    public Map getComponentState() { return componentState.asImmutable() }

    void setState(key, value) {
        synchronized(componentState) {
            componentState.put(key, value)
            stateUpdated = true
        }
    }

    public void bulkAdd(List<Document> docs, String contentType) {
        log.debug("[${this.id}] bulkAdd called with ${docs.size()} documents.")
        try {
            docs = prepareDocs(docs, contentType)
            log.debug("[${this.id}] Calling batchload on ${this.id} with batch of ${docs.size()}")
            batchLoad(docs)
            setState(LAST_UPDATED, new Date().getTime())
        } catch (Exception e) {
            log.error("[${this.id}] failed to add documents. (${e.message})", e)
            throw e
        }
    }

    @Override
    boolean handlesContent(String ctype) {
        return (ctype == "*/*" || !this.contentTypes || this.contentTypes.contains(ctype))
    }

    protected abstract void batchLoad(List<Document> docs);
}

