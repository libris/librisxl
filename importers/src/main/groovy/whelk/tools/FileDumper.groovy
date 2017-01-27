package whelk.tools

import groovy.util.logging.Slf4j
import groovyx.gpars.actor.DefaultActor
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.Iso2709Deserializer
import whelk.Document
import whelk.converter.MarcJSONConverter
import whelk.importer.MySQLLoader
import whelk.util.LegacyIntegrationTools

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Timestamp

/**
 * Created by theodortolstoy on 2017-01-24.
 */
@Slf4j
class FileDumper extends DefaultActor {

    BufferedWriter mainTableWriter
    BufferedWriter identifiersWriter
    MarcFrameConvertingActor converter

    FileDumper(exportFileName){
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
                        Map record = handleRowGroup(argument, converter)
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
                        else
                           println "Suppressed record"
                }
                catch (any) {
                    log.error any
                    return false
                }
                 }
        }
    }

    static Map handleRowGroup(List<VCopyDataRow> rows, marcFrameConverter) {
        try {
            VCopyDataRow row = rows.last()
            def authRecords = PostgresLoadfileWriter.getAuthDocsFromRows(rows)
            def document = null
            Timestamp timestamp = row.updated >= row.created ? row.updated : row.created
            Map doc = PostgresLoadfileWriter.getMarcDocMap(row.data)
            if (!PostgresLoadfileWriter.isSuppressed(doc)) {
                switch (row.collection) {
                    case 'auth':
                        document = convertDocument(marcFrameConverter, doc, row.collection, row.created)
                        break
                    case 'hold':
                        document = convertDocument(marcFrameConverter, doc, row.collection, row.created, PostgresLoadfileWriter.getOaipmhSetSpecs(row))
                        break
                    case 'bib':
                        SetSpecMatcher.matchAuthToBib(doc, authRecords)
                        document = convertDocument(marcFrameConverter, doc, row.collection, row.created, PostgresLoadfileWriter.getOaipmhSetSpecs(row))
                        break
                }
                return [collection: row.collection, document: document, isSuppressed: false, isDeleted: row.isDeleted, timestamp: timestamp]
            } else
                return [collection: row.collection, document: null, isSuppressed: true, isDeleted: row.isDeleted, timestamp: timestamp]
        }
        catch (any) {
            println any.message
        }
    }

    static Document convertDocument(converter, Map doc, String collection, Date created, List authData = null) {
        if (doc && !PostgresLoadfileWriter.isSuppressed(doc)) {
            String oldStyleIdentifier = "/" + collection + "/" + PostgresLoadfileWriter.getControlNumber(doc)

            def id = LegacyIntegrationTools.generateId(oldStyleIdentifier)

            Map convertedData = authData && authData.size() > 1 && collection != 'bib' ?
                    converter.sendAndWait([doc: doc, id: id, spec: [oaipmhSetSpecs: authData]]) :
                    converter.sendAndWait([doc: doc, id: id, spec: null])
            Document document = new Document(convertedData)
            document.created = created
            return document
        } else {
            println "is suppresse: ${PostgresLoadfileWriter.isSuppressed(doc)}"
            return null
        }
    }
}
