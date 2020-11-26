package whelk.importer

import groovy.util.logging.Log4j2 as Log
import org.apache.tools.tar.TarEntry
import org.apache.tools.tar.TarOutputStream
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.converter.JsonLDTurtleConverter
import whelk.util.ThreadPool
import java.util.zip.GZIPOutputStream

@Log
class SparqlLoader {

    static GZIPOutputStream gzOut
    static TarOutputStream tarOut
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
        int counter = 0
        startTime = System.currentTimeMillis()
        ThreadPool threadPool = new ThreadPool(Runtime.getRuntime().availableProcessors() * 2)
        List<Document> documents = []

        gzOut = new GZIPOutputStream(System.out)
        tarOut = new TarOutputStream(gzOut)
        try
        {
            for (document in whelk.storage.loadAll(null)) {
                if ( document.getDeleted() ) { continue }

                documents.add(document)
                counter++
                if (counter % BATCH_SIZE == 0) {
                    double docsPerSec = ((double) counter) / ((double) ((System.currentTimeMillis() - startTime) / 1000))
                    System.err.println("Converting $docsPerSec documents per second (running average since process start). Total count: $counter.")
                    threadPool.executeOnThread(documents, new BatchHandler())
                    documents = []
                }
            }

            if (documents.size() > 0) {
                threadPool.executeOnThread(documents, new BatchHandler())
            }
            threadPool.joinAll()
        } finally {
            tarOut.close()
            gzOut.close()
        }

        System.err.println("Done! $counter documents converted in ${(System.currentTimeMillis() - startTime) / 1000} seconds.")
    }

    private class BatchHandler implements ThreadPool.Worker<List<Document>> {
        void doWork(List<Document> documents, int threadIndex) {
            for (Document doc : documents) {
                String converted = converter.convert(doc.data, doc.getShortId())[JsonLd.NON_JSON_CONTENT_KEY]
                byte[] bytes = converted.getBytes("UTF-8")
                TarEntry entry = new TarEntry(doc.getShortId()+".ttl")
                entry.setSize(bytes.length)
                synchronized (tarOut) {
                    tarOut.putNextEntry(entry)
                    tarOut.write(bytes)
                    tarOut.closeEntry()
                }
            }
        }
    }
}
