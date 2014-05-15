package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import java.util.concurrent.*

import org.codehaus.jackson.map.ObjectMapper

import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.*

@Log
abstract class BasicComponent extends BasicPlugin implements Component {

    static final ObjectMapper mapper = new ObjectMapper()
    static final String LAST_UPDATED = "last_updated"

    static final String STATE_FILE_SUFFIX = ".state"

    protected Map componentState
    boolean stateUpdated

    Whelk whelk
    List contentTypes

    boolean master = false

    int batchUpdateSize = 1000 // Default. May be overriden by whelk.json

    // Plugins
    Map<String,FormatConverter> formatConverters = new HashMap<String,FormatConverter>()
    Map<String,DocumentSplitter> documentSplitters = new HashMap<String,DocumentSplitter>()
    List<LinkExpander> linkExpanders = new ArrayList<LinkExpander>()
    Map<String,Component> components = new HashMap<String,Component>()

    Listener listener
    Thread listenerThread = null
    Thread stateThread = null

    void init(String whelkId) {
        File stateFile= new File("${global.WHELK_WORK_DIR}/${whelkId}_${this.id}${STATE_FILE_SUFFIX}")
        if (stateFile.exists()) {
            log.info("Loading persisted state for [${this.id}].")
            try {
                componentState = mapper.readValue(stateFile, Map)
            } catch (Exception e) {
                log.error("[${this.id}] Failed to read statefile: ${e.message}", e)
                throw new WhelkRuntimeException("Couldn't read statefile for ${whelkId}/${this.id}", e)
            }
        } else {
            componentState = [:]
            componentState[LAST_UPDATED] = 0L
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
        // Look for components configured for whelk. Saves having to add component to all components in plugins.
        for (c in whelk.getComponents()) {
            components.put(c.id, c)
        }

        listener = plugins.find { it instanceof Listener }
        startStateThread(stateFile, whelkId)
        startListenerThread()
    }


    @Override
    public URI add(final Document document) {
        try {
            long updatetime = document.timestamp
            List<Document> docs = prepareDocs([document], document.contentType)
            batchLoad(docs)
            setState(LAST_UPDATED, updatetime)
            return new URI(document.identifier)
        } catch (Exception e) {
            log.error("[${this.id}] failed to add documents. (${e.message})", e)
            throw e
        }
    }

    public void bulkAdd(final List<Document> documents, String contentType, long updatetime = -1)  {
        log.debug("[${this.id}] bulkAdd called with ${documents.size()} documents.")
        try {
            if (updatetime < 0) {
                updatetime = new Date().getTime()
            }
            log.debug("Now is ${new Date().getTime()}, updatetime is $updatetime")
            def docs = prepareDocs(documents, contentType)
            log.debug("[${this.id}] Calling batchload on ${this.id} with batch of ${docs.size()}")
            batchLoad(docs)
            setState(LAST_UPDATED, updatetime)
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

    List<Document> prepareDocs(final List<Document> documents, String contentType) {
        FormatConverter fc = formatConverters.get(contentType)
        DocumentSplitter preSplitter = documentSplitters.get(contentType)

        DocumentSplitter postSplitter = documentSplitters.get(contentType)
        log.trace("fc: $fc")
        log.trace("postSplitter: $postSplitter")
        if (!postSplitter && fc) {
            postSplitter = documentSplitters.get(fc.resultContentType)
        }
        List docs = []
        if (fc || preSplitter || postSplitter) {
            for (doc in documents) {
                log.trace("[${this.id}] Calling prepare on doc ${doc.identifier}");
                if (preSplitter) {
                    log.trace("[${this.id}] Running preSplitter")
                    for (d in preSplitter.split(doc)) {
                        if (fc) {
                            log.trace(" ... with conversion")
                            docs << linkExpand(fc.convert(d))
                        } else {
                            log.trace(" ... without conversion")
                            docs << linkExpand(d)
                        }
                    }
                } else if (fc) {
                    log.trace("[${this.id}] Adding document after conversion.")
                    docs.add(linkExpand(fc.convert(doc)))
                } else {
                    log.trace("[${this.id}] Adding document without conversion.")
                    docs.add(linkExpand(doc))
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
        } else if (linkExpanders.size() > 0) {
            log.debug("Must loop over documents for link expansion.")
            for (doc in documents) {
                doc = linkExpand(doc)
            }
            log.debug("Links expanded.")
        } else {
            log.debug("No measures required. Returning document list.")
        }
        return documents
    }

    void runLinkExpanders(List docs) {
        for (doc in docs) {
            LinkExpander expander = getLinkExpanderFor(doc)
            if (expander) {
                log.debug("Expanding ${doc.identifier}")
                doc = expander.expand(doc)
            }
        }
    }

    Document linkExpand(Document doc) {
        LinkExpander le = getLinkExpanderFor(doc)
        if (le) {
            log.debug("Linkexpanding ${doc.identifier}")
            return le.expand(doc)
        }
        return doc
    }

    LinkExpander getLinkExpanderFor(Document doc) { return linkExpanders.find { it.valid(doc) } }

    public Map getState() { return componentState.asImmutable() }

    void setState(key, value) {
        this.componentState.put(key, value)
        stateUpdated = true
        if (key == LAST_UPDATED && listener) {
            log.trace("[${this.id}] Notifying listeners ..")
            listener.registerUpdate(this.id, value)
        }
    }

    long getLastUpdatedState() {
        return componentState.get(LAST_UPDATED, 0L)
    }

    void startStateThread(File stateFile, String whelkId) {
        log.info("[${this.id}] Starting thread to periodically save state.")
        if (!stateThread) {
            stateThread = Thread.start {
                boolean ok = true
                while(ok) {
                    try {
                        if (getState() && stateUpdated) {
                            log.trace("[${whelkId}-${this.id}] Saving state ...")
                            mapper.writeValue(stateFile, getState())
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
        }
    }

    void startListenerThread() {
        if (listener && listener.hasQueue(this.id) && !listenerThread) {

            listenerThread = Thread.start {
                log.debug("[${this.id}] Starting notification listener.");
                boolean ok = true
                while (ok) {
                    try {
                        log.debug("[${this.id}] Waiting for update ...");
                        ListenerEvent e = listener.nextUpdate(this.id)
                        log.debug("[${this.id}] Received update from component ${e.senderId} = ${e.payload}");
                        long lastUpdate = getLastUpdatedState()
                        log.trace("Lastupdate: $lastUpdate") 
                        log.trace("   Payload: ${e.payload}")
                        if (lastUpdate < e.payload) {
                            log.debug("[${this.id}] Component was last updated ${new Date(lastUpdate)} ($lastUpdate). Must catch up.");
                            def docs = []
                            int count = 0
                            long latestTimestamp = 0L
                            for (doc in components.get(e.senderId).getAll(null, new Date(lastUpdate), null)) {
                                if (doc.timestamp > latestTimestamp) {
                                    latestTimestamp = doc.timestamp
                                }
                                docs << doc
                                if ((++count % batchUpdateSize) == 0) {
                                    bulkAdd(docs, docs.first().contentType, latestTimestamp)
                                    docs = []
                                }
                            }
                            // Remainder
                            if (docs.size() > 0) {
                                log.debug("[${this.id}] Still ${docs.size()} documents left to process.")
                                bulkAdd(docs, docs.first().contentType, latestTimestamp)
                            }
                            log.debug("[${this.id}] Loaded $count document to get up to date with ${e.senderId}")
                        } else {
                            log.debug("[${this.id}] Already up to date.");
                        }
                    } catch (WhelkAddException wae) {
                        log.error("[${this.id}] Failed to add documents: ${wae.message}")
                    } catch (Exception e) {
                        log.error("[${this.id}] DISABLING LISTENER because of unexpected exception. Possible restart required.")
                        ok = false
                        listener.disable(this.id)
                        log.error("[${this.id}] Failed to read from notification queue: ${e.message}")
                        throw e
                    }
                }
            }
        } else if (!listenerThread) {
            log.debug("[${this.id}] No listener queue registered for me.")
        }
    }

}

