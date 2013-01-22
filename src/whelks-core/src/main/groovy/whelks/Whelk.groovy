package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log
import groovy.transform.Synchronized

import java.util.concurrent.*
import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.TeeOutputStream

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.api.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.whelks.imports.*
import se.kb.libris.whelks.persistance.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.plugin.external.*


@Log
class WhelkImpl extends BasicWhelk {

    WhelkImpl(String pfx) {
        super(pfx)
    }

    boolean belongsHere(Document d) {
        return !d.identifier || d.identifier.toString().startsWith("/"+this.prefix+"/")
    }

    URI store(Document d) {
        if (! belongsHere(d)) {
            throw new WhelkRuntimeException("Document does not belong here.")
        }
        try {
            log.info("[$prefix] Saving document with identifier $d.identifier")
            return super.store(d)
        } catch (WhelkRuntimeException wre) {
            log.error("Failed to save document ${d.identifier}: " + wre.getMessage())
        }

        return null
    }

    @Override
    void reindex() {
        int counter = 0
        Storage scomp = components.find { it instanceof Storage }
        Index icomp = components.find { it instanceof Index }
        IndexFormatConverter ifc = plugins.find { it instanceof IndexFormatConverter }

        log.info("Using $scomp as storage source for reindex.")

        long startTime = System.currentTimeMillis()
        List<Document> docs = new ArrayList<Document>()
        def executor = newScalingThreadPoolExecutor(1,50,60)
        try {
            for (Document doc : scomp.getAll()) {
                counter++
                docs.add(doc)
                if (counter % History.BATCH_SIZE == 0) {
                    long ts = System.currentTimeMillis()
                    log.info "(" + ((ts - startTime)/History.BATCH_SIZE) + ") New batch, indexing document with id: ${doc.identifier}. Velocity: " + (counter/((ts - startTime)/History.BATCH_SIZE)) + " documents per second. $counter total sofar."
                    def idocs = new ArrayList<Document>(docs)
                    executor.execute(new Runnable() {
                        public void run() {
                            if (ifc) {
                                idocs = ifc.convert(idocs)
                            }
                            log.debug("Current pool size: " + executor.getPoolSize() + " current active count " + executor.getActiveCount())
                            log.info("Indexing "+idocs.size()+" documents ... ")
                            icomp.index(idocs)
                        }
                    })
                    docs.clear()
                }
            }
            if (docs.size() > 0) {
                log.info "Indexing remaining " + docs.size() + " documents."
                if (ifc) {
                    docs = ifc.convert(docs)
                }
                icomp.index(docs)
            }
        } finally {
            executor.shutdown()
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow()
            }
        }
        println "Reindexed $counter documents"
    }

    @Override
    public Iterable<Document> log(Date since) {
        History historyComponent = null
        for (Component c : getComponents()) {
            if (c instanceof History) {
                return ((History)c).updates(since)
            }
        }
        throw new WhelkRuntimeException("Whelk has no index for searching");
    }

    public ExecutorService newScalingThreadPoolExecutor(int min, int max, long keepAliveTime) {
        ScalingQueue queue = new ScalingQueue()
        ThreadPoolExecutor executor = new ScalingThreadPoolExecutor(min, max, keepAliveTime, TimeUnit.SECONDS, queue)
        executor.setRejectedExecutionHandler(new ForceQueuePolicy())
        queue.setThreadPoolExecutor(executor)
        return executor
    }

}

class Tool {
    static Date parseDate(repr) {
        if (!repr.number) {
            return Date.parse("yyyy-MM-dd'T'hh:mm:ss", repr)
        } else {
            def tstamp = new Long(repr)
            if (tstamp < 0) // minus in days
                return new Date() + (tstamp as int)
            else // time in millisecs
                return new Date(tstamp)
        }
    }
}

@Log
class WhelkOperator {

    static main(args) {
        def operation = (args.length > 0 ? args[0] : null)
        def whelk = (args.length > 2 ? (new WhelkInitializer(new URI(args[2]).toURL().newInputStream()).getWhelks().find { it.prefix == args[1] }) : null)
        def resource = (args.length > 3 ? args[3] : whelk?.prefix)
        def since = (args.length > 4 ? Tool.parseDate(args[4]) : null)
        def numberOfDocs = (args.length > 5 ? args[5].toInteger() : null)
        long startTime = System.currentTimeMillis()
        long time = 0
        if (operation == "import") {
            def importer = new BatchImport(resource)
            def nrimports = importer.doImport(whelk, since, numberOfDocs)
            float elapsed = ((System.currentTimeMillis() - startTime) / 1000)
            println "Imported $nrimports documents in $elapsed seconds. That's " + (nrimports / elapsed) + " documents per second."

        } else if (operation == "reindex") {
            if (args.length > 3) { // Reindex a single document
                println "Reindexing document with identifier $resource"
                def document = whelk.get(new URI(resource))
                whelk.store(document)
            } else {
                whelk.reindex()
                println "Reindexed documents in " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds."
            }
        } else if (operation == "populate" || operation == "rebalance") {
            def target = (args.length > 2 ? (new WhelkInitializer(new URI(args[2]).toURL().newInputStream()).getWhelks().find { it.prefix == resource }) : null)
            int count = 0
            def docs = []
            for (doc in whelk.log()) {
                docs << doc
                count++
                if (count % 1000 == 0) {
                    log.info("Storing "+ docs.size()+ " documents in " + (target ? target.prefix : "all components") + " ... ($count total)")
                    if (target) {
                        target.store(docs)
                    } else {
                        whelk.store(docs)
                    }
                    docs = []
                }
            }
            if (docs.size() > 0) {
                count += docs.size()
                if (target) {
                    target.store(docs)
                } else {
                    whelk.store(docs)
                }
            }
            time = (System.currentTimeMillis() - startTime)/1000
            println "Whelk ${whelk.prefix} is ${operation}d. $count documents in $time seconds."
        } else if (operation == "benchmark") {
            int count = 0
            def docs = []
            for (doc in whelk.log()) {
                docs << doc
                count++
                if (count % 1000 == 0) {
                    time = (System.currentTimeMillis() - startTime)/1000
                    log.info("Retrieved "+ docs.size()+ " documents from $whelk ... ($count total). Time elapsed: ${time}. Current velocity: "+ (count/time) + " documents / second.")
                    docs = []
                }
            }
            time = (System.currentTimeMillis() - startTime)/1000
            log.info("$count documents read. Total time elapsed: ${time} seconds. That's " + (count/time) + " documents / second.")
        } else {
            println "Usage: <import|reindex|rebalance|populate> <whelkname> <config-url> [resource (for import)|target (for populate)] [since (for import)]"
        }
    }
}
