package whelk.importer

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.converter.JsonLDTurtleConverter
import whelk.util.ThreadPool

import java.nio.file.Files
import java.nio.file.Paths


@Log
class SparqlLoader {

    static final String DUMP_PATH = "/tmp/virtuosoload"
    static final int BATCH_SIZE = 1000

    Whelk whelk
    JsonLDTurtleConverter converter

    long startTime

    // Abort on unhandled exceptions, including those on worker threads.
    static
    {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            @Override
            void uncaughtException(Thread thread, Throwable throwable)
            {
                System.err.println("PANIC ABORT, unhandled exception:\n" + throwable.toString())
                throwable.printStackTrace()
                System.exit(-1)
            }
        })
    }

    SparqlLoader(Whelk w) {
        this.whelk = w
        this.converter = new JsonLDTurtleConverter()
    }

    void load() {

        println("Dumping segmented sparql data into: " + DUMP_PATH)
        Files.createDirectories(Paths.get(DUMP_PATH))

        int counter = 0
        startTime = System.currentTimeMillis()
        ThreadPool threadPool = new ThreadPool(Runtime.getRuntime().availableProcessors() * 2)
        List<Document> documents = []

        for (document in whelk.storage.loadAll(null)) {
            if ( document.getDeleted() ) { continue }

            documents.add(document)
            counter++
            if (counter % BATCH_SIZE == 0) {
                double docsPerSec = ((double) counter) / ((double) ((System.currentTimeMillis() - startTime) / 1000))
                println("Converting $docsPerSec documents per second (running average since process start). Total count: $counter.")
                threadPool.executeOnThread(documents, new BatchHandler())
                documents = []
            }
        }

        if (documents.size() > 0) {
            threadPool.executeOnThread(documents, new BatchHandler())
        }
        threadPool.joinAll()
        println("Done! $counter documents converted in ${(System.currentTimeMillis() - startTime) / 1000} seconds.")
    }

    private class BatchHandler implements ThreadPool.Worker<List<Document>> {
        void doWork(List<Document> documents, int threadIndex) {
            BufferedWriter writer = new BufferedWriter(new FileWriter(File.createTempFile("libris", ".ttl", new File(DUMP_PATH))))
            for (Document doc : documents) {
                String converted = converter.convert(doc.data, doc.getShortId())[JsonLd.NON_JSON_CONTENT_KEY]
                writer.write(converted)
            }
        }
    }
}
