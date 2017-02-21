package whelk.actors


import groovy.util.logging.Slf4j
import groovyx.gpars.actor.DefaultActor
import whelk.Document
import whelk.PostgresLoadfileWriter
import whelk.VCopyDataRow
import whelk.VCopyToWhelkConverter
import whelk.converter.marc.MarcFrameConverter
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

    FileDumper() {
        throw new Error("Groovy might let you call implicit default constructors, I will not.")
    }

    FileDumper(String exportFileName) {
        mainTableWriter = Files.newBufferedWriter(Paths.get(exportFileName), Charset.forName("UTF-8"))
        identifiersWriter = Files.newBufferedWriter(Paths.get(exportFileName + "_identifiers"), Charset.forName("UTF-8"))
        //threadPool = new ThreadPool(4 * Runtime.getRuntime().availableProcessors())
        threadPool = new ThreadPool(1)
    }

    public void close() {
        threadPool.joinAll()
        mainTableWriter.close()
        identifiersWriter.close()
    }

    public void convertAndWrite(List<List<VCopyDataRow>> batch) {
        threadPool.executeOnThread( batch, { _batch ->
            for ( List<VCopyDataRow> rowList in _batch) {
                Map recordMap = VCopyToWhelkConverter.convert(rowList)
                append(recordMap)
            }
        })
    }

    private synchronized void append(Map recordMap) {
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
                    "${doc.checksum.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString)}\t" +
                    "${doc.created}\n")

            for (String identifier : identifiers) {
                identifiersWriter.write("${doc.shortId}\t${identifier}\n")
            }
        }
    }
}
