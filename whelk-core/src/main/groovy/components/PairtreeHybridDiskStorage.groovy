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

    private long currentSequenceNumber = 0L

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
        index.createIndexIfNotExists(indexName)
        index.checkTypeMapping(indexName, "entry")

        currentSequenceNumber = index.loadHighestSequenceNumber(indexName)+1
        log.info("Hybrid storage current sequence number: $currentSequenceNumber")
    }

    @Override
    @groovy.transform.CompileStatic
    boolean store(Document doc) {
        boolean result = false
        try {
            result = super.store(doc)
            log.debug("Result from store()-operation: $result")
            if (result) {
                doc = setNewSequenceNumber(doc)
                index.index(doc.metadataAsJson.getBytes("utf-8"),
                    [
                        "index": ".libris",
                        "type": "entry",
                        "id": ((ElasticSearch)index).translateIdentifier(doc.identifier)
                    ]
                )
                index.flush()
            }
        } catch (Exception e) {
            throw new WhelkAddException("Failed to store ${doc.identifier}", e, [doc.identifier])
        }
        return result
    }

    @Override
    protected void batchLoad(List<Document> docs) {
        if (docs.size() == 1) {
            log.debug("Only one document to store. Using standard store()-method.")
            store(docs.first())
        } else {
            List<Map<String,String>> entries = []
            for (doc in docs) {
                boolean result = super.store(doc)
                if (result) {
                    doc = setNewSequenceNumber(doc)
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


    @Override
    Iterable<Document> getAll(String dataset = null, Date since = null, Date until = null) {
        if (dataset || since) {
            log.info("Loading documents by index query for dataset $dataset ${(since ? "since $since": "")}")
            def elasticResultIterator = index.metaEntryQuery(indexName, dataset, since, until)
            return new Iterable<Document>() {
                Iterator<Document> iterator() {
                    return new Iterator<Document>() {
                        public boolean hasNext() { elasticResultIterator.hasNext()}
                        public Document next() {
                            return super.get(elasticResultIterator.next())
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
        super.remove(uri)
        index.deleteFromEntry(uri, indexName)
    }

    @Override
    void rebuildIndex() {
        assert index
        int count = 0
        List<Map<String,String>> entries = []
        log.info("Started rebuild of metaindex for $indexName.")
        index.createIndexIfNotExists(indexName)
        index.checkTypeMapping(indexName, "entry")

        currentSequenceNumber = index.loadHighestSequenceNumber(indexName)+1
        for (document in getAllRaw()) {
            document = setNewSequenceNumber(document)
            entries << [
            "index":indexName,
            "type": "entry",
            "id": ((ElasticSearch)index).translateIdentifier(document.identifier),
            "data":((Document)document).metadataAsJson
            ]
            if (count++ % 20000 == 0) {
                index.index(entries)
                entries = []
            }
            if (log.isInfoEnabled() && count % 10000 == 0) {
                log.info("[${new Date()}] Rebuilding metaindex for $indexName. $count sofar.")
            }
        }
        if (entries.size() > 0) {
            index.index(entries)
        }
        log.info("Meta index rebuilt. Contains $count entries.")
    }
}
