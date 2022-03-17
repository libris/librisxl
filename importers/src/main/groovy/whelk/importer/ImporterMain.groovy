package whelk.importer

import java.lang.annotation.*
import java.util.concurrent.ExecutorService
import java.util.zip.GZIPOutputStream
import groovy.util.logging.Log4j2 as Log
import org.apache.commons.io.output.CountingOutputStream
import org.apache.commons.io.FilenameUtils

import whelk.Document
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.converter.JsonLdToTrigSerializer
import whelk.filter.LinkFinder
import whelk.reindexer.CardRefresher
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

    @Command(args='[SOURCE]')
    void importLinkedData(String source=null) {
        Whelk whelk = Whelk.createLoadedSearchWhelk(props)
        new LinkedDataImporter(whelk).importRdfAsMultipleRecords(source)
    }

    @Command(args='[FROM]')
    void reindexFrom(String from=null) {
        boolean useCache = true
        Whelk whelk = Whelk.createLoadedSearchWhelk(props, useCache)
        def reindex = new ElasticReindexer(whelk)
        long fromUnixTime = Long.parseLong(from)
        reindex.reindexFrom(fromUnixTime)
    }
    
    /**
     * The additional_types argument should be one of:
     * - comma-separated list of types to include. Like so: Person,GenreForm
     * - --all-types, to copy *all* types
     * - "", to be able to use --exclude-items without specifying additional types
     *
     * E.g., copy everything except items (holdings):
     * SOURCE_PROPERTIES RECORD_ID_FILE --all-types --exclude-items
     *
     * Copy only what's in RECORD_ID_FILE, but exclude items:
     * SOURCE_PROPERTIES RECORD_ID_FILE "" --exclude-items
     *
     * Copy what's in RECORD_ID_FILE and types MovingImageInstance and Map, don't exclude items:
     * SOURCE_PROPERTIES RECORD_ID_FILE MovingImageInstance,Map
     */
    @Command(args='SOURCE_PROPERTIES RECORD_ID_FILE [<ADDITIONAL_TYPES | --all-types | ""> [--exclude-items]]')
    void copywhelk(String sourcePropsFile, String recordsFile, additionalTypes=null, String excludeItems=null) {
        def sourceProps = new Properties()
        new File(sourcePropsFile).withInputStream { it
            sourceProps.load(it)
        }
        def source = Whelk.createLoadedCoreWhelk(sourceProps)
        def dest = Whelk.createLoadedSearchWhelk(props)
        def recordIds = new File(recordsFile).collect {
            it.split(/\t/)[0]
        }
        boolean shouldExcludeItems = excludeItems && excludeItems == '--exclude-items'
        def copier = new WhelkCopier(source, dest, recordIds, additionalTypes, shouldExcludeItems)
        copier.run()
    }

    // Records that do not cleanly translate to trig/turtle (bad data).
    // This obviously needs to be cleaned up!
    def trigExcludeList = [] as Set

    @Command(args='FILE [CHUNKSIZEINMB [--gzip]]')
    void lddbToTrig(String file, String chunkSizeInMB = null, String gzip = null, String collection = null) {
        def whelk = Whelk.createLoadedCoreWhelk(props)

        boolean writingToFile = file && file != '-'
        boolean shouldGzip = writingToFile && gzip && gzip == '--gzip'

        String chunkedFormatString = FilenameUtils.getFullPath(file) + FilenameUtils.getBaseName(file) + "-%04d" +
                (FilenameUtils.getExtension(file) ? "." + FilenameUtils.getExtension(file) : "") +
                (shouldGzip ? ".gz" : "")

        int partNumber = 1
        long maxChunkSizeInBytes = 0 // 0 = no limit
        if (chunkSizeInMB && chunkSizeInMB.toLong() > 0 && writingToFile) {
            maxChunkSizeInBytes = chunkSizeInMB.toLong() * 1000000
        }

        def outputStream
        if (writingToFile && maxChunkSizeInBytes > 0) {
            System.err.println("Writing ${String.format(chunkedFormatString, partNumber)}")
            outputStream = new FileOutputStream(String.format(chunkedFormatString, partNumber))
        } else if (writingToFile) {
            outputStream = new FileOutputStream(file)
        } else {
            outputStream = System.out
        }

        if (shouldGzip) {
            outputStream = new GZIPOutputStream(outputStream)
        }

        CountingOutputStream cos = new CountingOutputStream(outputStream)
        def ctx = whelk.jsonld.context
        def serializer = new JsonLdToTrigSerializer(ctx, cos)

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
                serializer.writeGraph(id, doc.data['@graph'])
            } catch (Throwable e) {
                // Part of the record may still have been written to the stream, which is now corrupt.
                System.err.println("${doc.getShortId()} conversion failed with ${e.toString()}")
            }

            if (writingToFile && maxChunkSizeInBytes > 0 && cos.getByteCount() > maxChunkSizeInBytes) {
                ++partNumber
                cos.close()
                System.err.println("Writing ${String.format(chunkedFormatString, partNumber)}")
                def fos = new FileOutputStream(String.format(chunkedFormatString, partNumber))
                if (shouldGzip) {
                    cos = new CountingOutputStream(new GZIPOutputStream(fos))
                } else {
                    cos = new CountingOutputStream(fos)
                }

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
