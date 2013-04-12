package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log
import groovy.transform.Synchronized

import org.elasticsearch.action.get.GetResponse

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.*


class ElasticStorageClient extends ElasticStorage implements Storage {}

@Log
abstract class ElasticStorage extends ElasticSearch {
    @Override
    public void store(Document doc, String idxpfx) {
        addDocument(doc, storageType, idxpfx)
    }
    @Override
    public void store(Iterable<Document> doc, String idxpfx) {
        addDocuments(doc, storageType, idxpfx)
    }
    @Override
    Document get(URI uri, String idxpfx) {
        log.debug("Received GET request for $uri")
        GetResponse response = performExecute(client.prepareGet(idxpfx, storageType, translateIdentifier(uri)).setFields("_source","_timestamp"))
        if (response && response.exists()) {
            def ts = (response.field("_timestamp") ? response.field("_timestamp").value : null)
            try {
                return new BasicDocument(response.sourceAsString())
            } catch (DocumentException de) {
                log.error("Failed to created document with uri ${uri} from source - " + de.getMessage(), de)
            }
        }
        return null
    }


    @Override
    Iterable<Document> getAll(String idxpfx) {
        return new ElasticIterable<Document>(this, idxpfx)
    }

    def loadAll(String idxpfx, String token = null, Date since = null, boolean loadDocuments = true, boolean sorted=false) {
        def results
        if (loadDocuments) {
            results = new ArrayList<Document>()
        } else {
            results = new ArrayList<LogEntry>()
        }
        def srb
        if (!token) {
            log.trace("Starting matchAll-query")
            srb = client.prepareSearch(idxpfx)
            if (loadDocuments) {
                srb = srb.addField("_source")
            }
            srb = srb.setTypes(storageType)
                .setScroll(TimeValue.timeValueMinutes(20))
                .setSize(History.BATCH_SIZE)
            if (sorted) {
                def query
                if (since) {
                    query = rangeQuery("_timestamp").gte(since.getTime())
                } else {
                    query = matchAllQuery()
                }
                srb = srb.addField("_timestamp")
                    .addSort("_timestamp", org.elasticsearch.search.sort.SortOrder.ASC)
                    .setQuery(query)
            } else {
                srb.setQuery(matchAllQuery())
            }
        } else {
            log.trace("Continuing query with scrollId $token")
            srb = client.prepareSearchScroll(token).setScroll(TimeValue.timeValueMinutes(2))
        }
        log.trace("loadAllquery: " + srb)
        def response = performExecute(srb)
        log.trace("Response: " + response)
        if (response.timedOut()) {
            log.warn("Response timed out")
        }
        if (response) {
            log.trace "Total log hits: ${response.hits.totalHits}"
            response.hits.hits.each {
                if (loadDocuments) {
                    try {
                        results.add(new BasicDocument(new String(it.source())))
                    } catch (DocumentException de) {
                        log.error("Failed to created document with id ${it.id} from source - " + de.getMessage(), de)
                    }
                } else {
                    results.add(new LogEntry(translateIndexIdTo(it.id, it.index), new Date(it.field("_timestamp").value)))
                }
            }
        } else if (!response || response.hits.length < 1) {
            log.info("No response recevied.")
        }
        log.debug("Found " + results.size() + " items. Scroll ID: " + response.scrollId())
        return [results, response.scrollId()]
    }

    OutputStream getOutputStreamFor(Document doc) {
        log.debug("Preparing outputstream for document ${doc.identifier}")
            return new ByteArrayOutputStream() {
                void close() throws IOException {
                    doc = doc.withData(toByteArray())
                    ElasticStorage.this.addDocument(doc, storageType)
                }
            }
    }

    @Override
    def Iterable<Document> updates(Date since) {
        return new ElasticIterable<Document>(this, since, true)
    }

}

@Log
class ElasticIterable<T> implements Iterable {
    def indexInstance
    Collection<T> list
    boolean incomplete = false
    boolean sorted
    def token
    String idxpfx
    Date since

    ElasticIterable(Index idx, String idxpfx, Date snc = null, boolean srt = false) {
        log.debug("Creating new iterable.")
        indexInstance = idx
        this.idxpfx = idxpfx
        since = snc
        sorted = srt
        (list, token) = indexInstance.loadAll(idxpfx, null, since, true, sorted)
        log.debug("Initial list with size: ${list.size} and token: $token")
        incomplete = (list.size == History.BATCH_SIZE)
    }

    Iterator<T> iterator() {
        return new ElasticIterator<T>()
    }

    class ElasticIterator<T> implements Iterator {

        Iterator iter

        ElasticIterator() {
            iter = list.iterator()
        }

        boolean hasNext() {
            return iter.hasNext()
        }

        void remove() {
            throw new UnsupportedOperationException("Not supported yet.")
        }

        @Synchronized
        T next() {
            T n = iter.next();
            iter.remove();
            if (!iter.hasNext() && incomplete) {
               refill()
            }
            return n
        }

        @Synchronized
        private void refill() {
            (list, token) = this.indexInstance.loadAll(this.idxpfx, token, since, true, sorted)
            incomplete = (list.size() == History.BATCH_SIZE)
            iter = list.iterator()
        }
    }
}
