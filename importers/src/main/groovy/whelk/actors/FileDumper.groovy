package whelk.actors


import groovy.util.logging.Slf4j
import whelk.Document
import whelk.VCopyDataRow
import whelk.VCopyToWhelkConverter
import whelk.component.PostgreSQLComponent
import whelk.converter.marc.MarcFrameConverter
import whelk.filter.LinkFinder
import whelk.util.ThreadPool

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Created by theodortolstoy on 2017-01-24.
 */
@Slf4j
    class FileDumper {

    BufferedWriter mainTableWriter
    BufferedWriter identifiersWriter
    ThreadPool threadPool

    /*
    In order to guard against the possibility that the conversion process may (one day?) no longer be reentrant,
    give each thread (slot) a converter of its' own (but share connection factory (PostgreSQLComponent)).
     */
    MarcFrameConverter[] converterPool

    FileDumper() {
        throw new Error("Groovy might let you call implicit default constructors, I will not.")
    }

    FileDumper(String exportFileName) {

        final int THREAD_COUNT = 4*Runtime.getRuntime().availableProcessors()
        final postgresqlComponent = new PostgreSQLComponent()

        mainTableWriter = Files.newBufferedWriter(Paths.get(exportFileName), Charset.forName("UTF-8"))
        identifiersWriter = Files.newBufferedWriter(Paths.get(exportFileName + "_identifiers"), Charset.forName("UTF-8"))
        threadPool = new ThreadPool(THREAD_COUNT)
        converterPool = new MarcFrameConverter[THREAD_COUNT]
        for (int i = 0; i < THREAD_COUNT; ++i)
            converterPool[i] = new MarcFrameConverter(new LinkFinder(postgresqlComponent))
    }

    public void close() {
        threadPool.joinAll()
        mainTableWriter.close()
        identifiersWriter.close()
    }

    public void convertAndWrite(List<List<VCopyDataRow>> batch) {
        threadPool.executeOnThread( batch, { _batch, threadIndex ->
            List<Map> writeBatch = []

            for ( List<VCopyDataRow> rowList in _batch) {
                Map recordMap = VCopyToWhelkConverter.convert(rowList, converterPool[threadIndex])
                writeBatch.add(recordMap)
            }

            append(writeBatch)
        })
    }

    private synchronized void append(List<Map> recordMaps) {
        for (Map recordMap in recordMaps) {

            if (recordMap && !recordMap.isSuppressed) {

                Document doc = recordMap.document
                String coll = recordMap.collection

                final String delimiterString = '\t'
                final String nullString = "\\N"


                List<String> identifiers = doc.recordIdentifiers

                mainTableWriter.write("${doc.shortId}\t" +
                        "${doc.dataAsString.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString)}\t" +
                        "${coll.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString)}\t" +
                        "${"vcopy"}\t" +
                        "${nullString}\t" +
                        "${recordMap.checksum.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString)}\t" +
                        "${doc.created}\n")

                for (String identifier : identifiers) {
                    identifiersWriter.write("${doc.shortId}\t${identifier}\n")
                }
            }
        }
    }
}
