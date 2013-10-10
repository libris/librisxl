package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.importers.*

@Log
class WhelkOperator {

    static main(args) {
        URI whelkConfigUri
        try {
            whelkConfigUri = new URI(System.getProperty("whelk.config.uri"))
        } catch (NullPointerException npe) {
            println "System property 'whelk.config.uri' is needed for whelkoperations."
            System.exit(1)
        }
        WhelkInitializer wi = new WhelkInitializer(whelkConfigUri.toURL().newInputStream())

        if (args.length > 2) {
            println "WhelkOperator doing ${args[0]} on ${args[1]}."
        }
        def operation = (args.length > 0 ? args[0] : null)
        def whelk = (args.length > 1 ? (wi.getWhelks().find { it.id == args[1] }) : null)
        def resource = (args.length > 2 ? args[2] : whelk?.id)
        def since
        def origin
        try {
            since = (args.length > 3 ? Tool.parseDate(args[3]) : null)
        } catch (java.text.ParseException pe) {
            origin = args[3]
        }
        def numberOfDocs = (args.length > 4 ? args[4].toInteger() : -1)
        boolean picky = (System.getProperty("picky") == "true")
        long startTime = System.currentTimeMillis()
        long time = 0
        if (operation == "import") {
            //def importer = new BatchImport(resource)
            def importer = new OAIPMHImporter(whelk, resource)
            def nrimports = importer.doImport(since, numberOfDocs, picky)
            def elapsed = ((System.currentTimeMillis() - startTime) / 1000)
            println "Imported $nrimports documents in $elapsed seconds. That's " + (nrimports / elapsed) + " documents per second."
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
        } else if (operation == "reindex") {
            if (args.length > 2) { // Reindex from a specific identifier
                whelk.reindex((String)null, resource)
            } else {
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
            println "Usage: <import|reindex|rebalance|populate> <whelkname> [resource (for import)|target (for populate)] [since (for import)]"
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
