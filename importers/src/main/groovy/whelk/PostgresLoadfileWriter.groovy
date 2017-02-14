package whelk

import groovy.sql.Sql
import groovy.util.logging.Slf4j as Log
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.Iso2709Deserializer
import whelk.converter.MarcJSONConverter
import whelk.importer.MySQLLoader
import whelk.actors.FileDumper
import whelk.actors.StatsMaker
import whelk.actors.StatsPrinter
import whelk.util.LegacyIntegrationTools

import java.sql.ResultSet
import java.sql.Statement
import groovyx.gpars.actor.DefaultActor
import java.sql.Timestamp


/**
 * Writes documents into a PostgreSQL load-file, which can be efficiently imported into lddb
 */
@Log
class PostgresLoadfileWriter {

    static void dumpAuthStats(String folderName, String connectionUrl) {
        def collection = 'bib'
        StatsMaker statsMaker = new StatsMaker()
        statsMaker.start()
        String sqlQuery = MySQLLoader.selectByMarcType[collection]
        List<Object> queryParameters = [0]
        dump(statsMaker, collection, connectionUrl, sqlQuery, queryParameters)
        statsMaker.stop()
    }

    static void "import"(DefaultActor actor, String collection, String connectionUrl, Date date) {
        String sqlQuery = MySQLLoader.selectHarvestByMarcType[collection]
        String dateString = date.toTimestamp().toString()
        List<Object> queryParameters = [0, dateString, dateString]
        dump(actor, collection, connectionUrl, sqlQuery, queryParameters)
    }


    static void "import"(DefaultActor actor, String collection, String connectionUrl, String[] vcopyIdsToImport) {
        String sqlQuery = MySQLLoader.selectExampleDataByMarcType[collection].replace('?', vcopyIdsToImport.collect { it -> '?' }.join(','))
        dump(actor, collection, connectionUrl, sqlQuery, vcopyIdsToImport.toList())
    }

    static void dumpGpars(String exportFileName, String collection, String connectionUrl) {
        String sqlQuery = MySQLLoader.selectByMarcType[collection]
        List<Object> queryParameters = [0]
        def fileDumper = new FileDumper(exportFileName)
        fileDumper.start()
        dump(fileDumper, collection, connectionUrl, sqlQuery, queryParameters)
        fileDumper.stop()
    }


    static void dump(DefaultActor suppliedActor, String collection, String connectionUrl, String sqlQuery, List<Object> queryParameters) {

        Sql sql = prepareSql(connectionUrl)

        StatsPrinter statsPrinter = new StatsPrinter()
        statsPrinter.start()


        def currentChunk = []
        List<VCopyDataRow> previousRowsInGroup = []
        VCopyDataRow previousRow = null
        VCopyDataRow currentRow

        sql.eachRow(sqlQuery, queryParameters) { ResultSet resultSet ->
            try {
                statsPrinter.sendAndWait([type: 'row'])
                currentRow = new VCopyDataRow(resultSet, collection)
                switch (previousRow) {
                    case null:                              //first run
                        previousRow = currentRow
                        previousRowsInGroup.add(currentRow)
                        break
                    case { collection == 'bib' && it.bib_id == currentRow.bib_id }://Same bib record
                        previousRowsInGroup.add(currentRow)
                        break
                    default:                                //new record
                        currentChunk.add(previousRowsInGroup)
                        if (currentChunk.count { it } == 500) {
                            currentChunk.each { c ->
                                suppliedActor.sendAndWait(c)
                                statsPrinter.sendAndWait([type: 'record'])
                            }
                            currentChunk = []
                        }
                        previousRow = currentRow
                        previousRowsInGroup = []
                        previousRowsInGroup.add(currentRow)
                        break
                }
            }
            catch (any) {
                println any.message
                println any.stackTrace
                //throw any // dont want to miss any records
            }
        }
        println "Done reading from DB. Processing last ${currentChunk.count { it }} ones."
        currentChunk.each { c ->
            suppliedActor.sendAndWait(c)
            statsPrinter.sendAndWait([type: 'record'])
        }

        statsPrinter.sendAndWait([type: 'report'])
        println "Done."
    }


    static Sql prepareSql(String connectionUrl) {
        def sql = Sql.newInstance(connectionUrl, "com.mysql.jdbc.Driver")
        sql.withStatement { Statement stmt -> stmt.fetchSize = Integer.MIN_VALUE }
        sql.connection.autoCommit = false
        sql.resultSetType = ResultSet.TYPE_FORWARD_ONLY
        sql.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY
        sql
    }

    static Map getMarcDocMap(byte[] data) {
        byte[] dataBytes = MySQLLoader.normalizeString(
                new String(data as byte[], "UTF-8"))
                .getBytes("UTF-8")

        MarcRecord record = Iso2709Deserializer.deserialize(dataBytes)

        if (record) {
            return MarcJSONConverter.toJSONMap(record)
        } else {
            return null
        }
    }

    static getAuthDocsFromRows(List<VCopyDataRow> rows) {
        rows.collect { it ->
            if (it.auth_id > 0) {
                return [bibid: it.bib_id,
                        id   : it.auth_id,
                        data : getMarcDocMap(it.authdata as byte[])]
            } else return null
        }
    }


    static List getOaipmhSetSpecs(resultSet) {
        List specs = []
        if (resultSet.collection == "bib") {
            int authId = resultSet.auth_id ?: 0
            if (authId > 0) {
                specs.add("authority:${authId}")
            }
        } else if (resultSet.collection == "hold") {
            if (resultSet.bib_id > 0)
                specs.add("bibid:${resultSet.bib_id}")
            if (resultSet.sigel)
                specs.add("location:${resultSet.sigel}")
        }
        return specs
    }

    static isSuppressed(Map doc) {
        def fields = doc.get("fields")
        for (def field : fields) {
            if (field.get("599") != null) {
                def field599 = field.get("599")
                if (field599.get("subfields") != null) {
                    def subfields = field599.get("subfields")
                    for (def subfield : subfields) {
                        if (subfield.get("a").equals("SUPPRESSRECORD"))
                            return true
                    }
                }
            }
        }
        return false
    }

    static String getControlNumber(Map doc) {
        def fields = doc.get("fields")
        for (def field : fields) {
            if (field.get("001") != null)
                return field.get("001")
        }
        return null
    }

    static Map handleRowGroup(List<VCopyDataRow> rows, marcFrameConverter) {
        VCopyDataRow row = rows.last()

        def authRecords = getAuthDocsFromRows(rows)
        def document = null
        Timestamp timestamp = row.updated >= row.created ? row.updated : row.created
        Map doc = getMarcDocMap(row.data)
        def controlNumber = getControlNumber(doc)
        if (!isSuppressed(doc)) {
            try {
                switch (row.collection) {
                    case 'auth':
                        document = convertDocument(marcFrameConverter, doc, row.collection, row.created)
                        break
                    case 'hold':
                        document = convertDocument(marcFrameConverter, doc, row.collection, row.created, getOaipmhSetSpecs(row))
                        break
                    case 'bib':
                        SetSpecMatcher.matchAuthToBib(doc, authRecords)
                        document = convertDocument(marcFrameConverter, doc, row.collection, row.created, getOaipmhSetSpecs(row))
                        break
                }
                return [collection: row.collection, document: document, isSuppressed: false, isDeleted: row.isDeleted, timestamp: timestamp, controlNumber: controlNumber]
            }
            catch (any) {
                println "ALLVARLIGT FEL! ${any.message}"
                println " ${any.stackTrace}"
                println "Bibid: ${row.bib_id}"
                return [collection: row.collection, document: null, isSuppressed: true, isDeleted: false, timestamp: timestamp, controlNumber: "0"]
            }

        } else
            return [collection: row.collection, document: null, isSuppressed: true, isDeleted: row.isDeleted, timestamp: timestamp, controlNumber: controlNumber]
    }

    static Document convertDocument(converter, Map doc, String collection, Date created, List authData = null) {
        if (doc && !isSuppressed(doc)) {
            String oldStyleIdentifier = "/" + collection + "/" + getControlNumber(doc)

            def id = LegacyIntegrationTools.generateId(oldStyleIdentifier)

            Map convertedData = authData && authData.size() > 1 ?
                    converter.sendAndWait([doc: doc, id: id, spec: [oaipmhSetSpecs: authData]]) :
                    converter.sendAndWait([doc: doc, id: id, spec: null])
            Document document = new Document(convertedData)
            document.created = created
            return document
        } else {
            println "is suppresse: ${isSuppressed(doc)}"
            return null
        }
    }


}





