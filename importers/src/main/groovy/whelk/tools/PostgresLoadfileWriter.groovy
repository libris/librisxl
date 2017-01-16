package whelk.tools

import groovy.json.JsonBuilder
import groovy.sql.Sql
import groovy.util.logging.Slf4j as Log
import groovyx.gpars.GParsPool
import groovyx.gpars.actor.Actors
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.Iso2709Deserializer
import whelk.Document
import whelk.converter.MarcJSONConverter
import whelk.converter.marc.MarcFrameConverter
import whelk.importer.MySQLLoader
import whelk.util.LegacyIntegrationTools

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
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
        StatsMaker statsMaker = new StatsMaker()
        statsMaker.start()
        StatsPrinter statsPrintingActor = new StatsPrinter()
        statsPrintingActor.start()

        Sql sql = prepareSql(connectionUrl)
        def currentChunk = []
        List<Map> previousRowsInGroup = []
        Map previousRow = null
        Map currentRow
        def collection = 'bib'

        //GParsPool.withPool { pool ->
        sql.eachRow(MySQLLoader.selectByMarcType[collection], [0]) { ResultSet resultSet ->
            try {
                currentRow = [data      : resultSet.getBytes('data'),
                              created   : resultSet.getTimestamp('create_date'),
                              collection: collection,
                              bib_id    : resultSet.getInt('bib_id'),
                              auth_id   : collection == 'bib' ? resultSet.getInt('auth_id') : 0,
                              authdata  : collection == 'bib' ? resultSet.getBytes('auth_data') : null,
                              sigel     : collection == "hold" ? resultSet.getString("shortname") : null]

                switch (previousRow) {
                    case null:                              //first run
                        previousRow = currentRow
                        previousRowsInGroup.add(currentRow)
                        break
                    case { it.bib_id == currentRow.bib_id }://Same bib record
                        previousRowsInGroup.add(currentRow)
                        break
                    default:
                        statsPrintingActor << [type: 'rowProcessed']
                        //new record
                        currentChunk.add(previousRowsInGroup)
                        if (currentChunk.size() >= 500) {
                            currentChunk.each { c ->
                                Map m = [doc     : getMarcDocMap(c.last().data as byte[]),
                                         authData: getAuthDocsFromRows(c as ArrayList)]
                                statsMaker.sendAndWait(m)

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
            }
        }
        //last ones
        currentChunk.each { c ->
            Map m = [doc     : getMarcDocMap(c.last().data as byte[]),
                     authData: getAuthDocsFromRows(c as ArrayList)]
            statsMaker.sendAndWait(m)
        }
        // }


    }

    static void "import"(DefaultActor actor, String collection, String connectionUrl, Date date) {
        String sqlQuery = MySQLLoader.selectHarvestByMarcType[collection]
        String dateString = date.toTimestamp().toString()
        List<Object> queryParameters = [0, dateString, dateString]

            dump(actor, collection, connectionUrl, sqlQuery, queryParameters)
    }


    static void "import"(DefaultActor actor, String collection, String connectionUrl, String[] vcopyIdsToImport) {
        String sqlQuery = MySQLLoader.selectExampleDataByMarcType[collection].replace('?',vcopyIdsToImport.collect{it->'?'}.join(','))
        dump(actor, collection, connectionUrl, sqlQuery, vcopyIdsToImport.toList())
    }

    static void dumpGpars(String exportFileName, String collection, String connectionUrl) {
        String sqlQuery = MySQLLoader.selectByMarcType[collection]
        List<Object> queryParameters = [0]
        def writeAct = Actors.actor {
            BufferedWriter mainTableWriter = Files.newBufferedWriter(Paths.get(exportFileName), Charset.forName("UTF-8"))
            BufferedWriter identifiersWriter = Files.newBufferedWriter(Paths.get(exportFileName + "_identifiers"), Charset.forName("UTF-8"))

            loop {
                react { argument ->
                    Document doc = argument.document
                    String coll = argument.collection
                    final String delimiter = '\t'
                    final String nullString = "\\N"

                    final delimiterString = new String(delimiter)

                    List<String> identifiers = doc.recordIdentifiers

                    mainTableWriter.write("${doc.shortId}\t${delimiter}\t${doc.dataAsString.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString)}\t${coll.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString)}\t${"vcopy"}\t${nullString}\t${doc.checksum.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString)}${delimiter}\t${doc.created}\n")

                    for (String identifier : identifiers) {
                        identifiersWriter.write("${doc.shortId}\t${identifier}\n")
                    }
                }
            }
        }

        dump(writeAct, collection, connectionUrl, sqlQuery, queryParameters)

    }


    static void dump(DefaultActor suppliedActor, String collection, String connectionUrl, String sqlQuery, List<Object> queryParameters) {

        Sql sql = prepareSql(connectionUrl)

        StatsPrinter statsPrintingActor = new StatsPrinter()
        statsPrintingActor.start()

        try {
            def converter = Actors.actor {
                MarcFrameConverter marcFrameConverter = new MarcFrameConverter()
                loop {
                    react { argument ->
                        try {
                            def doc = argument.spec == null ? marcFrameConverter.convert(argument.doc as Map, argument.id as String) :
                                    marcFrameConverter.convert(argument.doc as Map, argument.id as String, argument.spec as Map)
                            if (!doc) {
                                throw new Exception()
                            }
                            reply doc
                        }
                        catch (any) {
                            println any.message
                            println argument.inspect()
                            reply null
                        }
                    }
                }
            }
            def currentChunk = []
            List<Map> previousRowsInGroup = []
            Map previousRow = null
            Map currentRow
            //GParsPool.withPool { pool ->

                sql.eachRow(sqlQuery, queryParameters) { ResultSet resultSet ->


                    currentRow = [data      : resultSet.getBytes('data'),
                                  isDeleted   : resultSet.getBoolean('deleted'),
                                  created   : resultSet.getTimestamp('create_date'),
                                  updated   : resultSet.getTimestamp('update_date'),
                                  collection: collection,
                                  bib_id    : collection == 'bib' ? resultSet.getInt('bib_id'):0,
                                  auth_id   : collection == 'bib' ? resultSet.getInt('auth_id') : 0,
                                  authdata  : collection == 'bib' ? resultSet.getBytes('auth_data') : null,
                                  sigel     : collection == "hold" ? resultSet.getString("shortname") : null]

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
                            if (currentChunk.count{it} > 500) {
                                currentChunk.each { c ->
                                    try {
                                        Map a = handleRowGroup(c, converter)
                                        if (a && !a.isSuppressed) {
                                            suppliedActor.sendAndWait(a)
                                        }
                                    }
                                    catch (any) {
                                        println any.message
                                        println any.stackTrace
                                        println collection
                                        println c?.first()?.bib_id
                                        throw any // dont want to miss any records
                                    }
                                }
                                currentChunk = []
                            }
                            previousRow = currentRow
                            previousRowsInGroup = []
                            previousRowsInGroup.add(currentRow)
                            break
                    }
                    statsPrintingActor << [type: 'rowProcessed']
                }
                println "Last ones."
                currentChunk.each{ c ->
                    try {
                        Map a = handleRowGroup(c, converter)
                        if (a && !a.isSuppressed) {
                            suppliedActor.sendAndWait(a)
                        }
                    }
                    catch (any) {
                        println any.message
                        println any.stackTrace
                        throw any // dont want to miss any records

                    }

                }
            //} //With pool
        }
        catch (any) {
            println any.message
            println any.stackTrace
            throw any // dont want to miss any records
        }
        println "Done."
    }

    static getAuthDocsFromRows(List<Map> rows) {
        rows.collect { it ->
            if (it.auth_id > 0) {
                def authDoc = getMarcDocMap(it.authdata as byte[])
                return composeAuthData([bibid: it.bib_id, id: it.auth_id, data: authDoc])
            } else return null
        }
    }

    static Map handleRowGroup(List<Map> rows, marcFrameConverter) {
        def row = rows.last()
        def authRecords = getAuthDocsFromRows(rows)
        def document = null
        Timestamp timestamp = row.updated >= row.created ? row.updated : row.created
        Map doc = getMarcDocMap(row.data as byte[])
        if (!isSuppressed(doc)) {
            switch (row.collection) {
                case 'auth':
                    document = convertDocument(marcFrameConverter, doc, row.collection, row.created)
                    break
                case 'hold':
                    document = convertDocument(marcFrameConverter, doc, row.collection as String, row.created as Timestamp, getOaipmhSetSpecs(row))
                    break
                case 'bib':
                    SetSpecMatcher.matchAuthToBib(row, authRecords)
                    document = convertDocument(marcFrameConverter, doc, row.collection as String, row.created as Timestamp, getOaipmhSetSpecs(row))
                    break
            }
            return [collection: row.collection, document: document, isSuppressed: false,isDeleted: row.isDeleted, timestamp: timestamp ]
        } else
            return [collection: row.collection, document: null, isSuppressed: true, isDeleted: row.isDeleted, timestamp: timestamp ]

    }

    static Sql prepareSql(String connectionUrl) {
        def sql = Sql.newInstance(connectionUrl, "com.mysql.jdbc.Driver")
        sql.withStatement { Statement stmt -> stmt.fetchSize = Integer.MIN_VALUE }
        sql.connection.autoCommit = false
        sql.resultSetType = ResultSet.TYPE_FORWARD_ONLY
        sql.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY
        sql
    }


    static Map composeAuthData(LinkedHashMap<String, Object> map) {
        for (Map bibField in map.data.fields) {
            String key = bibField.keySet()[0]
            if (key.startsWith('1')) {
                map.field = key
                map.subfields = bibField[key].subfields
                return map
            }
        }
        def builder = new JsonBuilder(map)
        throw new Exception("Unhandled authority record ${builder.toPrettyString()} q")
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

    static Document convertDocument(converter, Map doc, String collection, Date created, List authData = null) {
        if (doc && !isSuppressed(doc)) {
            String oldStyleIdentifier = "/" + collection + "/" + getControlNumber(doc)

            def id = LegacyIntegrationTools.generateId(oldStyleIdentifier)

            Map convertedData = authData && authData.size() > 1 && collection != 'bib' ?
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

    private static boolean isSuppressed(Map doc) {
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

    private static String getControlNumber(Map doc) {
        def fields = doc.get("fields")
        for (def field : fields) {
            if (field.get("001") != null)
                return field.get("001")
        }
        return null
    }


}




