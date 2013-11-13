package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log
import groovy.util.CliBuilder

import se.kb.libris.whelks.importers.*

@Log
class WhelkOperator {

    static main(args) {
        InputStream whelkConfigInputStream = null
        InputStream pluginConfigInputStream = null
        try {
            whelkConfigInputStream = new URI(System.getProperty("whelk.config.uri")).toURL().newInputStream()
            pluginConfigInputStream = new URI(System.getProperty("plugin.config.uri")).toURL().newInputStream()
        } catch (NullPointerException npe) {
            println "System property 'whelk.config.uri' is needed for whelkoperations."
            System.exit(1)
        }
        WhelkInitializer wi = new WhelkInitializer(whelkConfigInputStream, pluginConfigInputStream)

        def cli = new CliBuilder(usage: 'whelkoperation')
        cli.o(longOpt:'operation', "which operation to perform (import|reindex|etc)", required:true, args: 1)
        cli.w(longOpt:'whelk', "the name of the whelk to perform operation on e.g. libris", required:true, args: 1)
        cli.d(longOpt:'dataset', "dataset (bib|auth|hold)", required:false, args:1)
        cli.u(longOpt:'serviceUrl', "serviceUrl for OAIPMH", required:false, args:1, argName:'URL')
        cli.s(longOpt:'since', "since Date (yyyy-MM-dd'T'hh:mm:ss) for OAIPMH", required:false, args:1)
        cli.n(longOpt:'num', "maximum number of document to import", required:false, args:1)
        cli.p(longOpt:'picky', "picky (true|false)", required:false, args:1)

        def opt = cli.parse(args)
        if (!opt) {
            return
        }
        def whelk = wi.getWhelks().find { it.id == opt.w }
        def resource = (opt.r ? opt.r : whelk)
        def operation = opt.o
        boolean picky = (System.getProperty("picky") == "true")
        long startTime = System.currentTimeMillis()
        long time = 0
        if (opt.o == "import") {
            if (!opt.d) {
                println "Dataset is required for OAIPMH import."
                return
            }
            def surl = (opt.u ? opt.u : null)
            def since = (opt.s ? Tool.parseDate(opt.s) : null)
            int nums = (opt.n ? opt.n.toInteger() : -1)
            println "Import from OAIPMH"
            //def importer = new BatchImport(resource)
            def importer = new OAIPMHImporter(whelk, opt.d, surl)
            def nrimports = importer.doImport(since, nums, picky)
            def elapsed = ((System.currentTimeMillis() - startTime) / 1000)
            println "Imported $nrimports documents in $elapsed seconds. That's " + (nrimports / elapsed) + " documents per second."
            /*
        } else if (operation == "importdump") {
            def importer = new DumpImporter(whelk, origin, picky)
            def nrimports = importer.doImportFromURL(new URL(resource))
            def elapsed = ((System.currentTimeMillis() - startTime) / 1000)
            println "Imported $nrimports documents in $elapsed seconds. That's " + (nrimports / elapsed) + " documents per second."
        } else if (operation == "importfile") {
            def importer = new DumpImporter(whelk, origin, picky)
            def nrimports = importer.doImportFromFile(new File(resource))
            def elapsed = ((System.currentTimeMillis() - startTime) / 1000)
            println "Imported $nrimports documents in $elapsed seconds. That's " + (nrimports / elapsed) + " documents per second."
            */
        } else if (opt.o == "reindex") {
            if (opt.d) { // Reindex from a specific dataset
                println "Reindex all documents in ${opt.d} in ${opt.w}"
                whelk.reindex(opt.dataset)
            } else {
                println "Reindex all documents in ${opt.w}"
                whelk.reindex()
                println "Reindexed documents in " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds."
            }
        } else if (operation == "rebuild") {
            try {
                whelk.rebuild(resource)
            } catch (groovy.lang.MissingMethodException me) {
                log.error("Rebuild can only be performed on CombinedWhelks.")
            }
        } else if (operation == "populate" || operation == "rebalance") {
            /*
            def target = (args.length > 2 ? (new WhelkInitializer(new URI(args[2]).toURL().newInputStream()).getWhelks().find { it.prefix == resource }) : null)
            int count = 0
            def docs = []
            for (doc in whelk.loadAll()) {
                docs << doc
                count++
                if (count % 1000 == 0) {
                    log.info("Storing "+ docs.size()+ " documents in " + (target ? target.prefix : "all components") + " ... ($count total)")
                    if (target) {
                        target.store(docs)
                    } else {
                        whelk.add(docs)
                    }
                    docs = []
                }
            }
            if (docs.size() > 0) {
                count += docs.size()
                if (target) {
                    target.store(docs)
                } else {
                    whelk.add(docs)
                }
            }
            time = (System.currentTimeMillis() - startTime)/1000
            println "Whelk ${whelk.prefix} is ${operation}d. $count documents in $time seconds."
            */
        } else if (operation == "benchmark") {
            int count = 0
            def docs = []
            for (doc in whelk.loadAll()) {
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
            println cli.usage()
        }
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
