package whelk.actors


import groovy.util.logging.Slf4j
import groovyx.gpars.actor.DefaultActor
import whelk.Document
import whelk.PostgresLoadfileWriter
import whelk.VCopyDataRow
import whelk.VCopyToWhelkConverter
import whelk.converter.marc.MarcFrameConverter

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
    MarcFrameConvertingActor converter

    FileDumper(String exportFileName) {
        mainTableWriter = Files.newBufferedWriter(Paths.get(exportFileName), Charset.forName("UTF-8"))
        identifiersWriter = Files.newBufferedWriter(Paths.get(exportFileName + "_identifiers"), Charset.forName("UTF-8"))
        converter = new MarcFrameConvertingActor(new MarcFrameConverter())
        converter.start()
    }

    void close() {
        mainTableWriter.flush()
        identifiersWriter.flush()
    }

    boolean append(List<VCopyDataRow> argument) {
        try {
            Map record = VCopyToWhelkConverter.convert(argument)
            if (record && !record.isSuppressed) {

                Document doc = record.document
                String coll = record.collection
                final String delimiter = '\t'
                final String nullString = "\\N"

                final delimiterString = new String(delimiter)

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
                return true
            } else {
                println "Suppressed record"
                return false
            }
        }
        catch (Exception any) {
            log.error("Error in FileDumper", any)
            return false
        }
    }


}
