package whelk.tools

import groovy.util.logging.Slf4j
import groovyx.gpars.actor.DefaultActor
import whelk.Document

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

    FileDumper(exportFileName){
        mainTableWriter = Files.newBufferedWriter(Paths.get(exportFileName), Charset.forName("UTF-8"))
        identifiersWriter = Files.newBufferedWriter(Paths.get(exportFileName + "_identifiers"), Charset.forName("UTF-8"))
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
            react { argument ->
                try {
                    Document doc = argument.document
                    String coll = argument.collection
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
                catch (any) {
                    log.error any
                    return false
                }
                 }
        }
    }
}
