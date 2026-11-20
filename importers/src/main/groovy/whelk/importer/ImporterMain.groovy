package whelk.importer

import java.lang.annotation.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern
import java.util.zip.GZIPOutputStream

import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes

import groovy.cli.picocli.CliBuilder
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j as Log

import org.apache.commons.io.output.CountingOutputStream
import org.apache.commons.io.FilenameUtils
import org.codehaus.groovy.runtime.typehandling.DefaultTypeTransformation

import whelk.Document
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.converter.JsonLdToTrigSerializer
import whelk.converter.RdfReader
import whelk.filter.LinkFinder
import whelk.reindexer.CardRefresher
import whelk.reindexer.ElasticReindexer
import whelk.util.Jackson
import whelk.util.PropertyLoader

@Log
class ImporterMain {

    private Properties props

    ImporterMain(String... propNames) {
        props = PropertyLoader.loadProperties(propNames)
    }

    def getWhelk() {
        return Whelk.createLoadedSearchWhelk(props)
    }

    @Command(args='SOURCE_URL [SUFFIXES]')
    void rdfDirToJsonLines(String sourceDir, String suffixes='rdf,ttl,jsonld') {
        var whelk = Whelk.createLoadedCoreWhelk(props)

        var systemContextUri = whelk.systemContextUri
        var baseUri = whelk.baseUri.toString()
        System.err.println "Whelk system context URI: $systemContextUri; base URI: $baseUri"

        var context = whelk.storage.loadDocumentByMainId(systemContextUri).data

        var sourceDirPath = Paths.get(sourceDir)
        var matcher = sourceDirPath.getFileSystem().getPathMatcher("glob:**/*.{$suffixes}")

        var printStream = System.out

        Files.walkFileTree(sourceDirPath, new SimpleFileVisitor<Path>() {
            @Override
            FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
                if (!matcher.matches(path)) {
                    return FileVisitResult.CONTINUE
                }

                var rdfSourcePath = path.toString()
                var data = new File(rdfSourcePath).withInputStream {
                    RdfReader.readRdf(it, rdfSourcePath, context, systemContextUri, baseUri)
                }

                if ('@graph' in data) {
                  // Ensure LDDB-expected order of things:
                  Map record = data['@graph'].find { it -> it['@type'] == 'Record' }
                  if (record) {
                    // assumes correctly structured record
                    def mainId = record['mainEntity']['@id']
                    Map mainEntity = data['@graph'].find { it -> it['@id'] == mainId }
                    List rest = data['@graph'].findAll { it -> !it.is(record) && !it.is(mainEntity) }
                    data = ['@graph': [record, mainEntity] + rest]
                  }
                }

                printStream.println(Jackson.mapper.writeValueAsString(data))

                return FileVisitResult.CONTINUE
            }
        })
    }

    @Command(args='SOURCE_URL DATASET_URI [DATASET_DESCRIPTION_FILE]',
             flags='--skip-index --replace-main-ids --force-delete --skip-dependers --allow-id-removal')
    void dataset(Map flags, String sourceUrl, String datasetUri, String datasetDescPath=null) {
        Whelk whelk = Whelk.createLoadedSearchWhelk(props)
        if (flags['skip-index']) {
            whelk.setSkipIndex(true)
        }
        if (flags['skip-dependers']) {
            whelk.setSkipIndexDependers(true)
        }
        if (flags['allow-id-removal']) {
            whelk.storage.doVerifyDocumentIdRetention = false
        }
        new DatasetImporter(whelk, datasetUri, flags, datasetDescPath).importDataset(sourceUrl)
    }

    @Command(args='DATASETS_DESCRIPTION_FILE [SOURCE_BASE_DIR] [DATASET_URI...]',
             flags='--skip-index --replace-main-ids --force-delete --skip-dependers')
    void datasets(Map flags, String datasetDescPath, String sourceBaseDir=null, String... onlyDatasets=null) {
        Whelk whelk = Whelk.createLoadedSearchWhelk(props)
        if (flags['skip-index']) {
            whelk.setSkipIndex(true)
        }
        if (flags['skip-dependers']) {
            whelk.setSkipIndexDependers(true)
        }
        DatasetImporter.loadDescribedDatasets(whelk, datasetDescPath, sourceBaseDir, onlyDatasets as Set, flags)
    }

    @Command(args='DATASET_URI...', flags='--force-delete')
    void dropDataset(Map flags, String... datasetUris) {
        Whelk whelk = Whelk.createLoadedSearchWhelk(props)
        for (datasetUri in datasetUris) {
            new DatasetImporter(whelk, datasetUri, flags).dropDataset()
        }
    }

    @Command(args='[COLLECTION] [-t NUMBEROFTHREADS]')
    void reindex(String... args) {
        def cli = new CliBuilder(
                usage: 'reindex [collection] -[ht]',
                header: '\n[collection] = auth|bib|hold|none|<dataset URI>|@type:<type>\n'
        )
        // Create the list of options.
        cli.with {
            h longOpt: 'help', 'Show usage information'
            t longOpt: 'threads', type: int,  'Number of threads in parallel'
        }
         
        def options = cli.parse(args)

        // Show usage text when -h or --help option is used.
        if (options.h) {
            cli.usage()
            
            return
        }
        
        String collection = null;
        if (options.arguments()) {
            collection = options.arguments()[0]
        }
        
        int numberOfThreads = Runtime.getRuntime().availableProcessors() * 2;
        if (options.t) {
            numberOfThreads = options.t
        }
        
        boolean useCache = true
        Whelk whelk = Whelk.createLoadedSearchWhelk(props, useCache)
        def reindex = new ElasticReindexer(whelk)
        reindex.reindex(collection, numberOfThreads)
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

    @Command(args='EMM_BASE_URL TYPE...', flags='--quick-create')
    void importEmm(Map flags, String emmBaseUrl, String... types) {
        Whelk whelk = Whelk.createLoadedSearchWhelk(props)
        boolean usingQuickCreate = false
        if (flags['quick-create']) {
            usingQuickCreate = true
        }
        def importer = new EmmImporter(whelk, emmBaseUrl, usingQuickCreate)
        importer.importFromLibrisEmm(types)
    }

    /**
     * The --additional-types argument can look like one of the following:
     * --additional-types=all              # Copy all types
     * --additional-types=none             # Don't copy additional types (default)
     * --additional-types=Person,GenreForm # Copy specific types
     *
     * You can also optionally copy historical document versions (from lddb__versions)
     * for certain types, all types, or not at all (default). "all" means
     * "copy versions of all records selected by RECORD_ID_FILE":
     * --copy-versions=all              # Copy history for every document added
     * --copy-versions=Instance,Person  # Copy history only for specific types
     * --copy-versions=none             # Don't copy history (default)
     *
     * E.g., copy everything except items (holdings):
     * SOURCE_PROPERTIES RECORD_ID_FILE --additional-types=all --exclude-items
     *
     * Copy only what's in RECORD_ID_FILE, but exclude items:
     * SOURCE_PROPERTIES RECORD_ID_FILE --additional-types=none --exclude-items
     *
     * Copy what's in RECORD_ID_FILE and types MovingImageInstance and Map, include items:
     * SOURCE_PROPERTIES RECORD_ID_FILE --additional-types=MovingImageInstance,Map
     *
     * Copy what's in RECORD_ID_FILE and types MovingImageInstance and Map, include items,
     * and copy historical versions of Instance and Map:
     * SOURCE_PROPERTIES RECORD_ID_FILE --additional-types=MovingImageInstance,Map --include-items --copy-versions=Instance,Map
     */
    @Command(args='SOURCE_PROPERTIES RECORD_ID_FILE [--additional-types=<types>] [--exclude-items | --dont-exclude-items] [--copy-versions=<types>]')
    void copywhelk(String sourcePropsFile, String recordsFile, String additionalTypes=null, String excludeItems=null, String copyVersions=null) {
        def sourceProps = new Properties()
        new File(sourcePropsFile).withInputStream { it
            sourceProps.load(it)
        }
        Whelk source = Whelk.createLoadedCoreWhelk(sourceProps)
        Whelk dest = Whelk.createLoadedSearchWhelk(props)
        List<String> recordIds = new File(recordsFile).collect {
            it.split(/\t/)[0]
        }
        boolean shouldExcludeItems = excludeItems && excludeItems == '--exclude-items'
        WhelkCopier copier = new WhelkCopier(source, dest, recordIds, additionalTypes, shouldExcludeItems, copyVersions)
        copier.run()
    }

    /**
     * Copy one or a few records, specified by XL id (or IRI) on the command line, from
     * another whelk, and index them. Like copywhelk, records that the specified records
     * link to, and records that link to them, are copied as well.
     *
     * Unlike copywhelk's --copy-versions, this one is boolean: if set, all
     * historical versions of the *specified records* (not of their deps) are copied.
     *
     * By default a specified record that already exists in the destination is skipped.
     * With --overwrite-existing its contents are instead overwritten (via an atomic
     * update) with the source version, and its dependencies/dependers are copied as normal.
     * Note that overwriting does not reconstruct version history, so --copy-versions
     * has no effect on records that were overwritten rather than freshly copied.
     *
     * The source whelk is configured with a properties file given by the system
     * property xl.importerSourceSecret.properties, e.g.:
     * -Dxl.importerSourceSecret.properties=/somewhere/source-secret.properties
     *
     * E.g., copy two records including their holdings and full history:
     * --include-items --copy-versions s93zr6j40v97d0f 3mfj87vf5t3zg3w
     */
    @Command(args='RECORD_ID...', flags='--copy-versions --include-items --overwrite-existing')
    void copyRecords(Map flags, String... recordIds) {
        if (!recordIds) {
            throw new IllegalArgumentException("At least one record id must be given.")
        }
        Whelk source = Whelk.createLoadedCoreWhelk(PropertyLoader.loadProperties("importerSourceSecret"))
        Whelk dest = Whelk.createLoadedSearchWhelk(props)
        def copier = new RecordCopier(source, dest, recordIds as List,
                (boolean) flags['copy-versions'], (boolean) flags['include-items'],
                (boolean) flags['overwrite-existing'])
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

        CountingOutputStream cos = new CountingOutputStream(bufferedTrigOutput(outputStream, shouldGzip))
        def ctx = whelk.jsonld.context
        // Only used for writing the prelude; records are serialized by the workers
        def serializer = new JsonLdToTrigSerializer(ctx, cos)
        serializer.prelude()
        serializer.flush()

        // Records are read from the database here, filtered and serialized in parallel by
        // the workers, and written by a single writer thread in the original order.
        int threads = Integer.getInteger('xl.lddbToTrig.threads', Runtime.getRuntime().availableProcessors())
        System.err.println("Serializing with ${threads} threads")
        ExecutorService workers = Executors.newFixedThreadPool(threads)
        ExecutorService writerExecutor = Executors.newSingleThreadExecutor()
        BlockingQueue<Future<List<byte[]>>> pending = new ArrayBlockingQueue<>(threads * 4)
        Future<List<byte[]>> endOfInput = CompletableFuture.completedFuture(null)
        ThreadLocal<TrigRecordSerializer> workerSerializers = ThreadLocal.withInitial {
            new TrigRecordSerializer(ctx)
        }

        Future writer = writerExecutor.submit {
            int i = 0
            while (true) {
                List<byte[]> records = pending.take().get()
                if (records == null) {
                    break
                }
                for (byte[] record : records) {
                    if (i % 500 == 0) {
                        System.err.println("$i records dumped.")
                    }
                    ++i
                    cos.write(record)

                    if (writingToFile && maxChunkSizeInBytes > 0 && cos.getByteCount() > maxChunkSizeInBytes) {
                        ++partNumber
                        serializer.flush()
                        cos.close()
                        System.err.println("Writing ${String.format(chunkedFormatString, partNumber)}")
                        def fos = new FileOutputStream(String.format(chunkedFormatString, partNumber))
                        cos = new CountingOutputStream(bufferedTrigOutput(fos, shouldGzip))

                        serializer.setOutputStream(cos)
                        // Make sure each chunk gets the prefixes
                        serializer.prelude()
                        serializer.flush()
                    }
                }
            }
            cos.close()
        }

        try {
            List<Document> batch = []
            for (Document doc : whelk.storage.loadAll(collection)) {
                if (doc.getShortId() in trigExcludeList) {
                    System.err.println("Excluding: ${doc.getShortId()}")
                    continue
                }
                batch.add(doc)
                if (batch.size() >= TRIG_BATCH_SIZE) {
                    List<Document> docs = batch
                    enqueueTrigBatch(pending, workers.submit({ workerSerializers.get().serialize(docs) } as Callable<List<byte[]>>), writer)
                    batch = []
                }
            }
            if (batch) {
                List<Document> docs = batch
                enqueueTrigBatch(pending, workers.submit({ workerSerializers.get().serialize(docs) } as Callable<List<byte[]>>), writer)
            }
            enqueueTrigBatch(pending, endOfInput, writer)
            writer.get()
        } finally {
            workers.shutdownNow()
            writerExecutor.shutdownNow()
        }
    }

    private static final int TRIG_BATCH_SIZE = 200

    // Like pending.put(), but gives up if the writer has died (which would otherwise block forever)
    private static void enqueueTrigBatch(BlockingQueue<Future<List<byte[]>>> pending, Future<List<byte[]>> batch, Future writer) {
        while (true) {
            if (writer.isDone()) {
                writer.get() // throws if the writer failed
                throw new IllegalStateException("TriG writer stopped unexpectedly")
            }
            if (pending.offer(batch, 1, TimeUnit.SECONDS)) {
                return
            }
        }
    }

    /**
     * Serializes records to TriG one at a time, for use by a single worker thread.
     */
    private static class TrigRecordSerializer {
        // The serializer numbers blank nodes it has to generate (for @reverse relations) with a
        // per-instance counter. Start each worker's counter in its own range so that labels stay
        // unique across the whole dump, as they were when a single serializer was used.
        private static final AtomicInteger instanceCount = new AtomicInteger()
        private static final int BNODE_RANGE = 100_000_000

        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(1 << 16)
        private final JsonLdToTrigSerializer serializer

        TrigRecordSerializer(Map context) {
            serializer = new JsonLdToTrigSerializer(context, buffer)
            serializer.setBnodeCounter(instanceCount.getAndIncrement() * BNODE_RANGE)
        }

        List<byte[]> serialize(List<Document> docs) {
            List<byte[]> result = new ArrayList<>(docs.size())
            for (Document doc : docs) {
                String id = doc.getCompleteId()
                filterProblematicData(id, doc.data)
                try {
                    serializer.writeGraph(id, doc.data.get('@graph'))
                } catch (Throwable e) {
                    // Part of the record may still have been written to the stream, which is now corrupt.
                    System.err.println("${doc.getShortId()} conversion failed with ${e.toString()}")
                }
                serializer.flush()
                result.add(buffer.toByteArray())
                buffer.reset()
            }
            return result
        }
    }

    // The TriG serializer writes many tiny strings, each of which PrintStream passes straight
    // through, so buffer before the (comparatively expensive per call) gzip/file layer.
    private static final int TRIG_BUFFER_SIZE = 1 << 16

    private static OutputStream bufferedTrigOutput(OutputStream out, boolean gzip) {
        if (gzip) {
            out = new GZIPOutputStream(out, TRIG_BUFFER_SIZE)
        }
        return new BufferedOutputStream(out, TRIG_BUFFER_SIZE)
    }

    @Command(args='[FROM]')
    void queueSparqlUpdatesFrom(String from=null) {
        Whelk whelk = Whelk.createLoadedSearchWhelk(props)
        long fromUnixTime = Long.parseLong(from)
        whelk.storage.queueSparqlUpdatesFrom(fromUnixTime)
    }

    @CompileStatic
    private static void filterProblematicData(String id, Object data) {
        if (data instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) data
            // Virtuoso bulk load doesn't like some unusual characters, such as 0x02,
            // so remove invisible control characters and unused code points
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                if (entry.value instanceof String) {
                    entry.value = cleanStringForVirtuoso((String) entry.value)
                }
            }
            map.entrySet().removeIf { Map.Entry<String, Object> entry ->
                String key = entry.key
                Object value = entry.value
                return key.startsWith("generic") ||
                        key == "marc:hasGovernmentDocumentClassificationNumber" ||
                        (key == "encodingLevel" && value instanceof String && ((String) value).contains(" ")) ||
                        !DefaultTypeTransformation.castToBoolean(value)
            }
            for (Object value : map.values()) {
                filterProblematicData(id, value)
            }
        } else if (data instanceof List) {
            List<Object> list = (List<Object>) data
            ListIterator<Object> it = list.listIterator()
            while (it.hasNext()) {
                Object value = it.next()
                if (value instanceof String) {
                    it.set(cleanStringForVirtuoso((String) value))
                }
            }
            if (list.removeIf { it == null }) {
                log.warn("Removing null value from ${id}")
            }
            for (Object value : list) {
                filterProblematicData(id, value)
            }
        }
    }

    private static final Pattern VIRTUOSO_UNSAFE_CHARS = Pattern.compile(/[\x00-\x08\x0B-\x0C\x0E-\x1F]/)

    @CompileStatic
    private static String cleanStringForVirtuoso(String data) {
        // Fast path: almost no strings contain control characters
        for (int i = 0; i < data.length(); i++) {
            if (data.charAt(i) < (char) 0x20) {
                return VIRTUOSO_UNSAFE_CHARS.matcher(data).replaceAll("")
            }
        }
        return data
    }

    static Map COMMANDS = getMethods().findAll {
        it.getAnnotation(Command)
    }.collectEntries { [it.name, cmddef(it)]}

    static Map cmddef(method) {
        def flagSpec = method.getAnnotation(Command).flags()
        if (flagSpec.indexOf('[') == -1) {
            flagSpec = flagSpec.split(/ /).findResults { it ? "[$it]" : null }.join(' ')
        }
        return [
            method: method,
            name: method.name,
            argSpec: method.getAnnotation(Command).args(),
            flagSpec: flagSpec,
        ]
    }

    static String cmdhelp(Map command) {
        return "\t${command.name} ${command.argSpec} ${command.flagSpec}"
    }

    static void help() {
        System.err.println "Usage: <program> COMMAND ARGS..."
        System.err.println()
        System.err.println("Available commands:")
        COMMANDS.values().sort().each {
            System.err.println cmdhelp(it)
        }
    }

    static void main(String... argv) {
        if (argv.length == 0) {
            help()
            System.exit(1)
        }

        def cmd = argv[0]
        def args = argv.length > 1 ? argv[1..-1] : []

        def command = COMMANDS[cmd]
        if (!command) {
            System.err.println "Unknown command: ${cmd}"
            help()
            System.exit(1)
        }

        def tool = new ImporterMain("secret")
        def arglist = []
        def flags = [:]
        try {
            if (command.flagSpec) {
                args.each {
                    if (it.startsWith('--')) {
                        if (command.flagSpec.indexOf("[$it]") == -1) {
                            throw new IllegalArgumentException("Uknown flag: ${it}")
                        }
                        flags[it.substring(2)] = true
                    } else {
                        arglist << it
                    }
                }
                tool."${command.name}"(flags, *arglist)
            } else {
                arglist = args
                tool."${command.name}"(*arglist)
            }
        } catch (IllegalArgumentException e) {
            System.err.println "Missing arguments. Expected:"
            System.err.println cmdhelp(command)
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
    String flags() default ''
}
