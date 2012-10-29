package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log
import groovy.transform.Synchronized

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
        IndexFormatConverter ifc = components.find { it instanceof IndexFormatConverter }

        long startTime = System.currentTimeMillis()
        List<Document> docs = new ArrayList<Document>()
        for (Document doc : scomp.getAll()) {
            counter++
            if (ifc) {
                Document cd = ifc.convert(doc)
                if (cd) {
                    docs << cd
                }
            } else {
                icomp.index(doc)
                docs << doc
            }
            if (counter % 10000 == 0) {
                long ts = System.currentTimeMillis()
                log.info "(" + ((ts - startTime)/1000) + ") New batch, indexing document with id: ${doc.identifier}. Velocity: " + (counter/((ts - startTime)/1000)) + " documents per second."
                icomp.index(docs)
                docs.clear()
            }
        }
        if (docs.size() > 0) {
            log.info "Indexing remaining " + docs.size() + " documents."
            icomp.index(docs)
        } 
        println "Reindexed $counter documents"
    }

    @Override
    public Iterable<LogEntry> log(Date since) {
        History historyComponent = null
        for (Component c : getComponents()) {
            if (c instanceof History) {
                return ((History)c).updates(since)
            }
        }
        throw new WhelkRuntimeException("Whelk has no index for searching");
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
            def whelk = new ReindexingWhelk(prefix)
            def date = (args.length > 2)? Tool.parseDate(args[2]) : null
            println "Using arguments: prefix=$prefix, resource=$resource, since=$date"
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
            def date = (args.length > 2)? Tool.parseDate(args[2]) : null
            def mode = (args.length > 3)? args[3]: "default"
            println "Using arguments: prefix=$prefix, resource=$resource, since=$date, mode=$mode"
            if (mode.equals("riak")) {
                whelk.addPlugin(new RiakStorage(prefix))
            } else {
                //whelk.addPlugin(new ElasticSearchClientStorageIndexHistory(prefix))
                //whelk.addPlugin(new DiskStorage("/tmp/whelk_storage"))
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
