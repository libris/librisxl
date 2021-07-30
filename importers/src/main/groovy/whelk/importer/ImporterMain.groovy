package whelk.importer

import whelk.reindexer.CardRefresher

import java.lang.annotation.*
import java.util.concurrent.ExecutorService
import org.apache.commons.io.output.CountingOutputStream

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.converter.JsonLdToTurtle
import whelk.filter.LinkFinder
import whelk.reindexer.ElasticReindexer
import whelk.util.PropertyLoader
import whelk.util.Tools

@Log
class ImporterMain {

    private Properties props

    ImporterMain(String... propNames) {
        props = PropertyLoader.loadProperties(propNames)
    }

    def getWhelk() {
        return Whelk.createLoadedSearchWhelk(props)
    }

    @Command(args='FNAME')
    void defs(String fname) {
        def whelk = new Whelk(new PostgreSQLComponent(props))
        DefinitionsImporter defsImporter = new DefinitionsImporter(whelk)
        defsImporter.definitionsFilename = fname
        defsImporter.run("definitions")
    }

    @Command(args='FNAME DATASET [--skip-index]')
    void dataset(String fname, String dataset, String skipIndexParam=null) {
        if (fname == '--skip-index' || dataset == '--skip-index' || (skipIndexParam && skipIndexParam != '--skip-index')) {
            throw new IllegalArgumentException("--skip-index must be third argument")
        }
        
        Whelk whelk = Whelk.createLoadedSearchWhelk(props)
        if (skipIndexParam == '--skip-index') {
            whelk.setSkipIndex(true)
        }
                
        DatasetImporter.importDataset(whelk, fname, dataset)
    }

    @Command(args='[COLLECTION]')
    void reindex(String collection=null) {
        boolean useCache = true
        Whelk whelk = Whelk.createLoadedSearchWhelk(props, useCache)
        def reindex = new ElasticReindexer(whelk)
        reindex.reindex(collection)
    }

    @Command(args='[COLLECTION]')
    void refreshCards(String collection=null) {
        Whelk whelk = Whelk.createLoadedSearchWhelk(props)
        new CardRefresher(whelk).refresh(collection)
    }

    @Command(args='[FROM]')
    void reindexFrom(String from=null) {
        boolean useCache = true
        Whelk whelk = Whelk.createLoadedSearchWhelk(props, useCache)
        def reindex = new ElasticReindexer(whelk)
        long fromUnixTime = Long.parseLong(from)
        reindex.reindexFrom(fromUnixTime)
    }

    static void sendToQueue(Whelk whelk, List doclist, ExecutorService queue, Map counters, String collection) {
        LinkFinder lf = new LinkFinder(whelk.storage)
        Document[] workList = new Document[doclist.size()]
        System.arraycopy(doclist.toArray(), 0, workList, 0, doclist.size())
        queue.execute({
            List<Document> storeList = []
            for (Document wdoc in Arrays.asList(workList)) {
                Document fdoc = lf.findLinks(wdoc)
                if (fdoc) {
                    counters["changed"]++
                    storeList << fdoc
                }
                counters["found"]++
                if (!log.isDebugEnabled()) {
                    Tools.printSpinner("Finding links. ${counters["read"]} documents read. ${counters["found"]} processed. ${counters["changed"]} changed.", counters["found"])
                } else {
                    log.debug("Finding links. ${counters["read"]} documents read. ${counters["found"]} processed. ${counters["changed"]} changed.")
                }
            }
            log.info("Saving ${storeList.size()} documents ...")
            whelk.storage.bulkStore(storeList, "xl", null, collection)
        } as Runnable)
    }
    
    /**
     * The additional_types argument should be a comma separated list of types to include. Like so:
     * Person,GenreForm
     */
    @Command(args='SOURCE_PROPERTIES RECORD_ID_FILE [ADDITIONAL_TYPES]')
    void copywhelk(String sourcePropsFile, String recordsFile, additionalTypes=null) {
        def sourceProps = new Properties()
        new File(sourcePropsFile).withInputStream { it
            sourceProps.load(it)
        }
        def source = Whelk.createLoadedCoreWhelk(sourceProps)
        def dest = Whelk.createLoadedSearchWhelk(props)
        def recordIds = new File(recordsFile).collect {
            it.split(/\t/)[0]
        }
        def copier = new WhelkCopier(source, dest, recordIds, additionalTypes)
        copier.run()
    }

    // Records that do not cleanly translate to trig/turtle (bad data).
    // This obviously needs to be cleaned up!
    def trigExcludeList = [
            "b5qzm6mgd220mqq2", // @vocab:ISO639-2-T "sqi" ;
            "htf4scsmk0n44qcq",
            "b7f0m6mgd2q54bpd",
            "m7g8xhxrp1f0lp7l",
            "5qgtg1g97195wsm3",
            "0qgn9v9421qh0jts",
            "f8j3q9qkh342fhmg",
            "7pcwj3jc92jmfccp",
            "w0qj6r61z3kkbsrj",
            "hdx4scsmk3c2bm5r",
            "mw38xhxrp1s0b85x",
            "1rvnbwb530j6k6nj",
            "q8lc1l1vs3b7168t",
            "mcd8xhxrp5k5zm44",
            "qmjc1l1vs0hp5mck",
            "1zbpbwb5313nlh3g",
            "wbvj6r61z4zl0108",
            "m468xhxrp3w9svn4",
            "csk1n7nhf52nd4br",
            "nq89zjzsq2fj5cjs"
    ] as Set

    @Command(args='FILE [CHUNKSIZEINMB]')
    void lddbToTrig(String file, String chunkSizeInMB = null, String collection = null) {
        def whelk = Whelk.createLoadedCoreWhelk(props)

        def ctx = JsonLdToTurtle.parseContext([
                '@context': whelk.jsonld.context
        ])
        def opts = [useGraphKeyword: false, markEmptyBnode: true]

        boolean writingToFile = file && file != '-'

        String chunkedFormatString = "%04d-%s"

        int partNumber = 1
        long maxChunkSizeInBytes = 0 // 0 = no limit
        if (chunkSizeInMB && chunkSizeInMB.toLong() > 0 && writingToFile) {
            maxChunkSizeInBytes = chunkSizeInMB.toLong() * 1000000
        }

        def outputStream
        if (writingToFile && maxChunkSizeInBytes > 0) {
            System.err.println("Writing ${String.format(chunkedFormatString, partNumber, file)}")
            outputStream = new FileOutputStream(String.format(chunkedFormatString, partNumber, file))
        } else if (writingToFile) {
            outputStream = new FileOutputStream(file)
        } else {
            outputStream = System.out
        }

        CountingOutputStream cos = new CountingOutputStream(outputStream)
        def serializer = new JsonLdToTurtle(ctx, cos, opts)
        serializer.prelude()
        int i = 0
        for (Document doc : whelk.storage.loadAll(collection)) {
            if (doc.getShortId() in trigExcludeList) {
                System.err.println("Excluding: ${doc.getShortId()}")
                continue
            }

            def id = doc.completeId
            if (i % 500 == 0) {
                System.err.println("$i records dumped.")
            }
            ++i
            filterProblematicData(id, doc.data)
            try {
                serializer.objectToTrig(id, doc.data)
            } catch (Throwable e) {
                // Part of the record may still have been written to the stream, which is now corrupt.
                System.err.println("${doc.getShortId()} conversion failed with ${e.toString()}")
            }

            if (writingToFile && maxChunkSizeInBytes > 0 && cos.getByteCount() > maxChunkSizeInBytes) {
                ++partNumber
                cos.close()
                System.err.println("Writing ${String.format(chunkedFormatString, partNumber, file)}")
                cos = new CountingOutputStream(new FileOutputStream(String.format(chunkedFormatString, partNumber, file)))
                serializer.setOutputStream(cos)
                // Make sure each chunk gets the prefixes
                serializer.prelude()
            }
        }
        cos.close()
    }

    @Command(args='[FROM]')
    void queueSparqlUpdatesFrom(String from=null) {
        Whelk whelk = Whelk.createLoadedSearchWhelk(props)
        long fromUnixTime = Long.parseLong(from)
        whelk.storage.queueSparqlUpdatesFrom(fromUnixTime)
    }

    private static void filterProblematicData(id, data) {
        if (data instanceof Map) {
            data.removeAll { entry ->
                return entry.key.startsWith("generic") || entry.key.equals("marc:hasGovernmentDocumentClassificationNumber")
            }
            data.keySet().each { property ->
                filterProblematicData(id, data[property])
            }
        } else if (data instanceof List) {
            if (data.removeAll([null])) {
                log.warn("Removing null value from ${id}")
            }
            data.each {
                filterProblematicData(id, it)
            }
        }
    }

    static COMMANDS = getMethods().findAll { it.getAnnotation(Command)
                                    }.collectEntries { [it.name, it]}

    static void help() {
        System.err.println "Usage: <program> COMMAND ARGS..."
        System.err.println()
        System.err.println("Available commands:")
        COMMANDS.values().sort().each {
            System.err.println "\t${it.name} ${it.getAnnotation(Command).args()}"
        }
    }

    static void main(String... args) {
        if (args.length == 0) {
            help()
            System.exit(1)
        }

        def cmd = args[0]
        def arglist = args.length > 1? args[1..-1] : []

        def method = COMMANDS[cmd]
        if (!method) {
            System.err.println "Unknown command: ${cmd}"
            help()
            System.exit(1)
        }

        def main
        if (cmd.startsWith("vcopy")) {
            main = new ImporterMain("secret", "mysql")
        } else {
            main = new ImporterMain("secret")
        }

        try {
            main."${method.name}"(*arglist)
        } catch (IllegalArgumentException e) {
            System.err.println "Missing arguments. Expected:"
            System.err.println "\t${method.name} ${method.getAnnotation(Command).args()}"
            System.err.println e.message
            org.codehaus.groovy.runtime.StackTraceUtils.sanitize(e).printStackTrace()
            System.exit(1)
        }
    }

}

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@interface Command {
    String args() default ''
}
