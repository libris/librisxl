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
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.persistance.*
import se.kb.libris.whelks.imports.*


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
class ReindexingWhelk extends WhelkImpl {

    ReindexingWhelk(pfx) {
        super(pfx)
        log.info("Starting whelk '$pfx' in standalone reindexing mode.")
    }

    static main(args) {
        if (args) {
            def prefix = args[0]
            def resource = (args.length > 1 ? args[1] : args[0])
            def basedir =  (args.length > 2 ? args[2] : "/extra/whelk_storage")
            def whelk = new ReindexingWhelk(prefix)
            println "Using arguments: prefix=$prefix, resource=$resource, basedir=$basedir"
            whelk.addPlugin(new DiskStorage(basedir, prefix))
            whelk.addPlugin(new ElasticSearchClientStorageIndexHistory(prefix))
            whelk.addPlugin(new MarcCrackerAndLabelerIndexFormatConverter())
            long startTime = System.currentTimeMillis()
            whelk.reindex()
            println "Reindexed documents in " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds."
        } else {
            println "Supply whelk-prefix and resource-name as arguments to commence reindexing."
        }
    }
}

@Log
class ImportWhelk extends BasicWhelk {

    ImportWhelk(pfx) {
        super(pfx)
        log.info("Starting whelk '$pfx' in standalone import mode.")
    }

    static main(args) {
        if (args) {
            def prefix = args[0]
            def resource = (args.length > 1 ? args[1] : args[0])
            def whelk = new ImportWhelk(prefix)
            // Mode parameter doubles as basedir for diskstorage if not using riak.
            def mode = (args.length > 2 ? args[2] : "/extra/whelk_storage")
            def date = (args.length > 3 ? Tool.parseDate(args[3]) : null)
            println "Using arguments: prefix=$prefix, resource=$resource, mode=$mode, since=$date"
            if (mode.equals("riak")) {
                whelk.addPlugin(new RiakStorage(prefix))
            } else {
                //whelk.addPlugin(new RiakStorage(prefix))
                whelk.addPlugin(new DiskStorage(mode, prefix))
                whelk.addPlugin(new ElasticSearchClientStorageIndexHistory(prefix))
                whelk.addPlugin(new MarcCrackerAndLabelerIndexFormatConverter())
            }
            def importer = new se.kb.libris.whelks.imports.BatchImport(resource)
            long startTime = System.currentTimeMillis()
            def nrimports = importer.doImport(whelk, date)
            float elapsed = ((System.currentTimeMillis() - startTime) / 1000)
            println "Imported $nrimports documents in $elapsed seconds. That's " + (nrimports / elapsed) + " documents per second."
        } else {
            println "Supply whelk-prefix and resource-name as arguments to commence import."
        }
    }
}
