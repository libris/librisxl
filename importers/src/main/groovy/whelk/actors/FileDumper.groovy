package whelk.actors


import groovy.util.logging.Slf4j
import groovyx.gpars.actor.DefaultActor
import whelk.Document
import whelk.PostgresLoadfileWriter
import whelk.VCopyDataRow

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Created by theodortolstoy on 2017-01-24.
 */
@Slf4j
class FileDumper extends DefaultActor {

    BufferedWriter mainTableWriter
    BufferedWriter identifiersWriter
    MarcFrameConvertingActor converter

    FileDumper(String exportFileName){
        mainTableWriter = Files.newBufferedWriter(Paths.get(exportFileName), Charset.forName("UTF-8"))
        identifiersWriter = Files.newBufferedWriter(Paths.get(exportFileName + "_identifiers"), Charset.forName("UTF-8"))
        converter = new MarcFrameConvertingActor()
        converter.start()
    }


    void afterStart() {
       println "FileDumper started."
    }

    void afterStop() {
        println "FileDumper stopping. Flushing writers"
        mainTableWriter.flush()
        identifiersWriter.flush()
        println "Writers flushed"
    }

    @Override
    protected void act() {
        loop {
            react { List<VCopyDataRow> argument ->
                try {
                        Map record = PostgresLoadfileWriter.handleRowGroup(argument, converter)
                       if(record && !record.isSuppressed) {

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
                           reply true
                       }
                        else {
                           println "Suppressed record"
                           return true
                       }
                }
                catch (Exception any) {
                    log.error("Error in FileDumper",any)
                    return false
                }
                 }
        }
    }


}
