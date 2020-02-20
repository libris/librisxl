package whelk.importer

import whelk.reindexer.CardRefresher

import java.lang.annotation.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import groovy.util.logging.Log4j2 as Log
import groovy.sql.Sql

import whelk.Document
import whelk.Whelk
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.converter.JsonLdToTurtle
import whelk.filter.LinkFinder
import whelk.importer.DefinitionsImporter
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

    @Command(args='[COLLECTION] [no-embellish|no-cache]')
    void reindexToStdout(String collection=null, String directive=null) {
        if (directive == null && collection?.indexOf('-') > -1) {
            directive = collection
            collection = null
        }
        boolean useCache = directive != 'no-cache'
        boolean doEmbellish = directive != 'no-embellish'

        Whelk whelk = Whelk.createLoadedCoreWhelk(props, useCache)

        println "Creating whelk with dummy ElasticSearch"
        println "- collection: $collection"
        println "- directive: $directive"
        println "- useCache: $useCache"
        println "- doEmbellish: $doEmbellish"

        whelk.elastic = new ElasticSearch("", "", "") {
            Tuple2<Integer, String> performRequest(String method,
                    String path, String body, String contentType0 = null) {
                println "PATH: $path, CONTENT_TYPE: $contentType0, SIZE: ${body.size()}"
                println body
                return new Tuple2(-1, "{}")
            }

            void embellish(Whelk w, Document src, Document copy) {
                if (doEmbellish) {
                    super.embellish(w, src, copy)
                }
            }
        }

        def reindex = new ElasticReindexer(whelk)
        reindex.reindex(collection)
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

    @Command(args='FILE')
    void lddbToTrig(String file, String collection) {
        def whelk = Whelk.createLoadedCoreWhelk(props)

        def ctx = JsonLdToTurtle.parseContext([
                '@context': whelk.jsonld.context
        ])
        def opts = [useGraphKeyword: false, markEmptyBnode: true]

        def handleSteam = !file || file == '-' ? { it(System.out) }
                            : new File(file).&withOutputStream
        handleSteam { out ->
            def serializer = new JsonLdToTurtle(ctx, out, opts)
            serializer.prelude()
            int i = 0
            for (Document doc : whelk.storage.loadAll(collection)) {
                def id = doc.completeId
                System.err.println "[${++i}] $id"
                serializer.objectToTrig(id, doc.data)
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
