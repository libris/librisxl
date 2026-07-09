package whelk.importer

import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import org.apache.commons.io.output.CountingOutputStream
import org.apache.commons.io.FilenameUtils
import groovy.transform.CompileStatic
import static groovy.transform.TypeCheckingMode.SKIP
import groovy.util.logging.Log4j2 as Log

import whelk.JsonLd
import whelk.converter.JsonLdToTrigSerializer
import whelk.util.Jackson

@Log
@CompileStatic
class TrigFileDumper {

    JsonLdToTrigSerializer serializer
    CountingOutputStream cos

    String chunkedFormatString
    boolean shouldGzip
    boolean writingToFile
    int i = 0
    int partNumber = 1
    long maxChunkSizeInBytes = 0 // 0 = no limit

    TrigFileDumper(Map ctx, String file, String chunkSizeInMB, boolean shouldGzip) {
        writingToFile = file && file != '-'
        this.shouldGzip = shouldGzip && writingToFile

        chunkedFormatString = FilenameUtils.getFullPath(file) + FilenameUtils.getBaseName(file) + "-%04d" +
                (FilenameUtils.getExtension(file) ? "." + FilenameUtils.getExtension(file) : "") +
                (shouldGzip ? ".gz" : "")

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

        cos = new CountingOutputStream(outputStream)

        serializer = new JsonLdToTrigSerializer(ctx, cos)
        serializer.prelude()
    }

    void dump(String id, Map data) {
        if (i % 500 == 0) {
            System.err.println("$i records dumped.")
        }

        ++i
        filterProblematicData(id, data)
        try {
            serializer.writeGraph(id, data['@graph'])
        } catch (Throwable e) {
            // Part of the record may still have been written to the stream, which is now corrupt.
            System.err.println("${id} conversion failed with ${e.toString()}")
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

    void close() {
        cos.close()
    }

    @CompileStatic(SKIP)
    private static void filterProblematicData(id, data) {
        if (data instanceof Collection) {
            data.eachWithIndex { it, index ->
                if (it instanceof String) {
                    // Virtuoso bulk load doesn't like some unusual characters, such as 0x02,
                    // so remove invisible control characters and unused code points
                    data[index] = it.replaceAll(/\p{C}/, "")
                }
            }
        }

        if (data instanceof Map) {
            data.removeAll { entry ->
                return entry.key.startsWith("generic") ||
                        entry.key.equals("marc:hasGovernmentDocumentClassificationNumber") ||
                        (entry.key.equals("encodingLevel") && entry.value instanceof String && entry.value.contains(" ")) ||
                        !entry.value
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

    public static void main(String[] args) {
        String inFile = args[0]
        String outFile = args[1]
        String ctxFile = args[2]
        Map contextData = Jackson.mapper.readValue(new File(ctxFile), Map)
        var ctx = JsonLd.getNormalizedContext(contextData)
        var trigDumper = new TrigFileDumper(ctx, outFile, "1000", true)
        new File(inFile).withInputStream { ins ->
            if (inFile.endsWith('.gz')) {
                ins = new GZIPInputStream(ins)
            }
            ins.eachLine { line ->
                Map data = Jackson.mapper.readValue(line, Map)
                String id = ((Map) ((List) data['@graph'])[0])['@id']
                trigDumper.dump(id, data)
            }
            trigDumper.close()
        }
    }
}
