package se.kb.libris.whelks.component

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import groovy.transform.Synchronized

import java.util.concurrent.*

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.BasicPlugin
import se.kb.libris.whelks.plugin.Plugin

import se.kb.libris.conch.Tools

import gov.loc.repository.pairtree.Pairtree

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.*

import com.google.common.io.Files

class PairtreeHybridDiskStorage extends PairtreeDiskStorage implements HybridStorage {

    Index index
    String indexName

    boolean rebuilding = false

    static Logger log = LoggerFactory.getLogger(PairtreeHybridDiskStorage.class)

    PairtreeHybridDiskStorage(Map settings) {
        super(settings)
    }

    @Override
    void init(String stName) {
        super.init(stName)
        index = plugins.find { it instanceof Index }
        if (!index) {
            throw new PluginConfigurationException("HybridStorage requires Index component.")
        }
        indexName = "."+stName
    }

    void start() {
        super.start()
        index.createIndexIfNotExists(indexName)
        index.checkTypeMapping(indexName, "entry")
    }

    @Override
    @groovy.transform.CompileStatic
    boolean store(Document doc) {
        if (rebuilding) { throw new DownForMaintenanceException("The system is currently rebuilding it's indexes. Please try again later.") }
        boolean result = false
        result = super.store(doc)
        log.debug("Result from store()-operation: $result")
        if (result) {
            //doc = setNewSequenceNumber(doc)
            index.index(doc.metadataAsJson.getBytes("utf-8"),
                [
                    "index": ".libris",
                    "type": "entry",
                    "id": ((ElasticSearch)index).translateIdentifier(doc.identifier)
                ]
            )
            index.flush()
        }
        return result
    }

    @Override
    protected void batchLoad(List<Document> docs) {
        if (rebuilding) { throw new DownForMaintenanceException("The system is currently rebuilding it's indexes. Please try again later.") }
        if (docs.size() == 1) {
            log.debug("Only one document to store. Using standard store()-method.")
            store(docs.first())
        } else {
            List<Map<String,String>> entries = []
            for (doc in docs) {
                boolean result = super.store(doc)
                if (result) {
                    //doc = setNewSequenceNumber(doc)
                    entries << [
                        "index":indexName,
                        "type": "entry",
                        "id": ((ElasticSearch)index).translateIdentifier(doc.identifier),
                        "data":((Document)doc).metadataAsJson
                    ]
                }
            }
            index.index(entries)
            index.flush()
        }
    }

    /*
    private Document setNewSequenceNumber(Document doc) {
        doc.entry['sequenceNumber'] = currentSequenceNumber
        increaseSequenceNumber()
        return doc
    }

    private void increaseSequenceNumber() {
        synchronized (currentSequenceNumber) {
            currentSequenceNumber++
        }
    }
    */


    @Override
    Iterable<Document> getAll(String dataset = null, Date since = null, Date until = null) {
        if (rebuilding) { throw new DownForMaintenanceException("The system is currently rebuilding it's indexes. Please try again later.") }
        if (dataset || since) {
            log.debug("Loading documents by index query for dataset $dataset ${(since ? "since $since": "")}")
            def elasticResultIterator = index.metaEntryQuery(indexName, dataset, since, until)
            return new Iterable<Document>() {
                Iterator<Document> iterator() {
                    return new Iterator<Document>() {
                        public boolean hasNext() { elasticResultIterator.hasNext()}
                        public Document next() {
                            String nextIdentifier = elasticResultIterator.next()
                            Document nextDocument = super.get(nextIdentifier)
                            if (!nextDocument) {
                                throw new WhelkRuntimeException("Document ${nextIdentifier} not found in storage.")
                            }
                            return nextDocument
                        }
                        public void remove() { throw new UnsupportedOperationException(); }
                    }
                }
            }
        }
        return getAllRaw(dataset)
    }
    @Override
    void remove(URI uri) {
        if (rebuilding) { throw new DownForMaintenanceException("The system is currently rebuilding it's indexes. Please try again later.") }
        super.remove(uri)
        index.deleteFromEntry(uri, indexName)
    }

    @Override
    void rebuildIndex() {
        assert index
        rebuilding = true
        int diskCount = 0
        List<Map<String,String>> entryList = []
        log.info("Started rebuild of metaindex for $indexName.")
        index.createIndexIfNotExists(indexName)
        index.checkTypeMapping(indexName, "entry")

        for (document in getAllRaw()) {
            entryList << [
            "index":indexName,
            "type": "entry",
            "id": ((ElasticSearch)index).translateIdentifier(document.identifier),
            "data":((Document)document).metadataAsJson
            ]
            if (diskCount++ % 2000 == 0) {
                index.index(entryList)
                entryList = []
            }
            if (log.isInfoEnabled() && diskCount % 50000 == 0) {
                log.info("[${new Date()}] Rebuilding metaindex for $indexName. $diskCount sofar.")
            }
        }
        if (entryList.size() > 0) {
            index.index(entryList)
        }
        index.flush()
        log.info("Created $diskCount entries.")
        //updateSequenceNumbers()
        rebuilding = false
    }

    /*
    void updateSequenceNumbers() {
        log.info("Sorting entries for sequenceNumbers.")
        rebuilding = true
        int page = 0
        int indexCount = 0
        currentSequenceNumber = 0
        List entries = index.loadEntriesInOrder(indexName, "entry", page)
        while (entries.size() > 0) {
            for (entry in entries) {
                def sourceMap = entry.remove("data")
                sourceMap.get("entry").put("sequenceNumber", ++currentSequenceNumber)
                entry.put("data", mapper.writeValueAsString(sourceMap))
                indexCount++
            }
            if (log.isInfoEnabled() && indexCount % 50000 == 0) {
                log.info("[${new Date()}] Setting documentSequence for $indexName. $indexCount sofar.")
            }
            index.index(entries)
            entries = index.loadEntriesInOrder(indexName, "entry", ++page)
        }
        log.info("Meta index sequence ordered. Contains $indexCount entries.")
        rebuilding = false
    }
    */
}
