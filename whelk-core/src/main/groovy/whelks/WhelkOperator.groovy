package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log
import groovy.util.CliBuilder

import se.kb.libris.whelks.importers.*

@Log
class WhelkOperator {

    static String LOCKFILE_NAME = "whelkoperator.lck"
    static String DEFAULT_LOCKFILE_PATH = "/var/run/whelkoperator"

    static main(args) {
        File lockFile = lockFile()
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

        def cli = new CliBuilder(usage: 'whelkoperation', posix:true)
        cli.o(longOpt:'operation', "which operation to perform (import|reindex|rebuild|etc)", required:true, args: 1)
        cli.w(longOpt:'whelk', "the name of the whelk to perform operation on e.g. libris", required:true, args: 1)
        cli.d(longOpt:'dataset', "dataset (e.g. bib|auth|hold)", required:false, args:1)
        cli.u(longOpt:'serviceUrl', "URL for imports. Can be an OAIPMH service URL, the URL of a dump, or a filename.", required:false, args:1, argName:'URL')
        cli.s(longOpt:'since', "since Date (yyyy-MM-dd'T'hh:mm:ss) for OAIPMH", required:false, args:1)
        cli.n(longOpt:'num', "maximum number of document to import", required:false, args:1)
        cli.p(longOpt:'picky', "picky (true|false)", required:false, args:1)
        cli.c(longOpt:'component', "which component to use (defaults to 'all')", required: false, args: 1)
        cli._(longOpt:'fromStorage', 'used for rebuild. from which storage to read source data.', required:false, args:1, argName:'storage id')
        cli._(longOpt:'silent', 'used by imports. If silent, the spinner is not shown during imports.', required:false)

        def opt = cli.parse(args)
        if (!opt) {
            log.trace("Deleting lockfile ${lockFile.absolutePath}.")
            lockFile.delete()
            return
        }
        def whelk = wi.getWhelks().find { it.id == opt.w }
        def operation = opt.o
        boolean picky = (System.getProperty("picky") == "true")
        long startTime = System.currentTimeMillis()
        long time = 0
        if (opt.o == "import") {
            if (!opt.d) {
                println "Dataset is required for import."
                log.trace("Deleting lockfile ${lockFile.absolutePath}.")
                lockFile.delete()
                return
            }
            def importer = null
            if (opt.c) {
                importer = whelk.getImporter(opt.c)
            } else {
                def importers = whelk.getImporters()
                if (importers.size() > 1) {
                    println "Multiple importers available for ${whelk.id}, you need to specify one with the '-c' argument."
                        log.trace("Deleting lockfile ${lockFile.absolutePath}.")
                        lockFile.delete()
                        return
                } else {
                    try {
                        importer = importers[0]
                    } catch (IndexOutOfBoundsException e) { }
                }
            }
            if (!importer) {
                println "Couldn't find any importers working for ${whelk.id}."
                log.trace("Deleting lockfile ${lockFile.absolutePath}.")
                lockFile.delete()
                return
            }
            int nrimports = 0
            int nums = (opt.n ? opt.n.toInteger() : -1)
            log.info("Importer name: ${importer.getClass().getName()}")
            if (importer.getClass().getName() == "se.kb.libris.whelks.importers.OAIPMHImporter") {
                if (opt.u) {
                    importer.serviceUrl = opt.u
                }
                Date since = (opt.s ? Tool.parseDate(opt.s) : null)
                println "Import from OAIPMH"
                nrimports = importer.doImport(opt.d, nums, opt.silent, picky, since)
            } else {
                if (!opt.u) {
                    println "URL is required for import."
                    log.trace("Deleting lockfile ${lockFile.absolutePath}.")
                    lockFile.delete()
                    return
                }
                nrimports = importer.doImport(opt.d, nums, opt.silent, picky, new URL(opt.u))
            }
            long elapsed = ((System.currentTimeMillis() - startTime) / 1000)
            if (nrimports > 0 && elapsed > 0) {
                println "Imported $nrimports documents in $elapsed seconds. That's " + (nrimports / elapsed) + " documents per second."
            } else {
                println "Nothing imported ..."
            }

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
            def selectedComponents = null
            if (opt.c) {
                selectedComponents = opt.c.split(",") as List<String>
            }
            if (opt.d) { // Reindex from a specific dataset
                println "Reindex all documents in ${opt.d} in ${opt.w} into components: ${(opt.c ? selectedComponents : 'all of them')}"
                whelk.reindex(opt.dataset)
            } else {
                println "Reindex all documents in ${opt.w} into components: ${(opt.c ? selectedComponents : 'all of them')}"
                whelk.reindex(null, selectedComponents)
                println "Reindexed documents in " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds."
            }
        } else if (operation == "rebuild") {
            if (opt.fromStorage) {
                String dataset = (opt.dataset ? opt.dataset : null)
                whelk.reindex(dataset, (opt.c ? opt.c.split(",") : null), opt.fromStorage)
            } else {
                println cli.usage()
            }
        } else if (operation == "linkfindandcomplete") {
            if (!opt.dataset) {
                println cli.usage()
            } else {
                println "Running linkfinders and filters for ${opt.dataset} in ${opt.whelk}"
                def ds = (opt.dataset == "all" ? null : opt.dataset)
                //whelk.findLinks(ds)
                whelk.runFilters(ds)
            }
        } else if (operation == "benchmark") {
            int count = 0
            def docs = []
            for (doc in whelk.loadAll((opt.d ? opt.d : null))) {
                docs << doc
                count++
                if (count % 1000 == 0) {
                    time = (System.currentTimeMillis() - startTime)/1000
                    println("Retrieved "+ docs.size()+ " documents from $whelk ... ($count total). Time elapsed: ${time}. Current velocity: "+ (count/time) + " documents / second.")
                    docs = []
                }
            }
            time = (System.currentTimeMillis() - startTime)/1000
            println("$count documents read. Total time elapsed: ${time} seconds.")
        } else {
            println cli.usage()
        }
        log.trace("Deleting lockfile ${lockFile.absolutePath}.")
        lockFile.delete()
    }

    static File lockFile() {
        File lockFile
        [DEFAULT_LOCKFILE_PATH+"/"+LOCKFILE_NAME, LOCKFILE_NAME].eachWithIndex() { fn, n ->
            if (!lockFile) {
                lockFile = new File(fn)
                if (lockFile.exists()) {
                    println("")
                    println("Lock file ${lockFile.absolutePath} found. Cancelling operation.")
                    System.exit(0)
                }
                log.trace("checking ${lockFile.absolutePath} ...")
                try {
                    lockFile.createNewFile()
                    log.trace("File created.")
                    if (n > 0) {
                        println("")
                        println("Warning: Failed to create lockfile in $DEFAULT_LOCKFILE_PATH. Setting lockfile locally.")
                    }
                } catch (IOException ioe) {
                    log.debug("Unable to write file.")
                    lockFile = null
                }
            }
        }
        if (!lockFile) {
            println("")
            println("Fatal: Unable to write lock file.")
            System.exit(1)
        }
        return lockFile
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
