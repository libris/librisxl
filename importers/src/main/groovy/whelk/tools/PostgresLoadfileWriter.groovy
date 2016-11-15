package whelk.tools

import groovy.sql.Sql
import groovy.util.logging.Slf4j as Log
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

//import static groovyx.gpars.dataflow.Dataflow.task
//import static groovyx.gpars.GParsPool.withPool
/**
 * Writes documents into a PostgreSQL load-file, which can be efficiently imported into lddb
 */
@Log
class PostgresLoadfileWriter {
    private static
    final int THREAD_COUNT = Runtime.runtime.availableProcessors();
    private static final int CONVERSIONS_PER_THREAD = 100;

    // USED FOR DEV ONLY, MUST _NEVER_ BE SET TO TRUE ONCE XL GOES INTO PRODUCTION. WITH THIS SETTING THE IMPORT WILL
    // _SKIP_ DOCUMENTS THAT FAIL CONVERSION, RESULTING IN POTENTIAL DATA LOSS IF USED WHEN IMPORTING TO A PRODUCTION XL
    private static final boolean FAULT_TOLERANT_MODE = false;

    private static MarcFrameConverter s_marcFrameConverter;
    private static BufferedWriter s_mainTableWriter;
    private static BufferedWriter s_identifiersWriter;
    private static Thread[] s_threadPool;
    private static Vector<String> s_failedIds = new Vector<String>();

    // Abort on unhandled exceptions, including those on worker threads.
    static
    {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            @Override
            void uncaughtException(Thread thread, Throwable throwable) {
                System.out.println("PANIC ABORT, unhandled exception:\n");
                throwable.printStackTrace();
                System.exit(-1);
            }
        });
    }

    public
    static void dumpGpars(String exportFileName, String collection, String connectionUrl) {
        if (FAULT_TOLERANT_MODE)
            System.out.println("\t**** RUNNING IN FAULT TOLERANT MODE, DOCUMENTS THAT FAIL CONVERSION WILL BE SKIPPED.\n" +
                    "\tIF YOU ARE IMPORTING TO A PRODUCTION XL, ABORT NOW!! AND RECOMPILE WITH FAULT_TOLERANT_MODE=false");

        s_marcFrameConverter = new MarcFrameConverter();
        s_mainTableWriter = Files.newBufferedWriter(Paths.get(exportFileName), Charset.forName("UTF-8"));
        s_identifiersWriter = Files.newBufferedWriter(Paths.get(exportFileName + "_identifiers"), Charset.forName("UTF-8"));
        def counter = 0
        def startTime = System.currentTimeMillis()
        //withPool {
        try {
            def sql = Sql.newInstance(connectionUrl, "com.mysql.jdbc.Driver")
            sql.withStatement { Statement stmt -> stmt.fetchSize = Integer.MIN_VALUE }
            sql.connection.autoCommit = false
            sql.resultSetType = ResultSet.TYPE_FORWARD_ONLY
            sql.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY

            List<Map> previousAuthData = []
            Map previousBibResultSet = null

            sql.eachRow(MySQLLoader.selectByMarcType[collection], [0]) { ResultSet currentRow ->
                Map rowMap = [data      : currentRow.getBytes('data'),
                              created   : currentRow.getTimestamp('create_date'),
                              collection: collection]


                if (++counter % 1000 == 0) {
                    def elapsedSecs = (System.currentTimeMillis() - startTime) / 1000
                    if (elapsedSecs > 0) {
                        def docsPerSec = counter / elapsedSecs
                        println "Working. Currently ${counter} documents saved. Crunching ${docsPerSec} docs / s"
                    }
                }


                switch (collection) {
                    case 'auth':
                        handleRow(rowMap, collection, null)
                        break
                    case 'hold':
                        rowMap.put('sigel', currentRow.getString("shortname"))
                        handleRow(rowMap, collection, getHoldOaipmhSetSpecs(rowMap))
                        break
                    case 'bib':
                        int currentRecordId = currentRow.getInt('bib_id')
                        rowMap.put('bib_id', currentRecordId)
                        rowMap.put('auth_id', currentRow.getInt('auth_id'))
                        Map currentAuthData = rowMap.auth_id > 0 ?
                                [bibid: rowMap.bib_id,
                                 id   : rowMap.auth_id,
                                 data : getMarcDocMap(currentRow.getBytes('auth_data'))] : null
                        if (rowMap.auth_id > 0 && currentAuthData.data.fields != null) {
                            currentAuthData << composeAuthData(currentAuthData)
                        }

                        switch (previousBibResultSet) {
                        //first run
                            case null:
                                previousBibResultSet = rowMap
                                previousAuthData << currentAuthData
                                break
                        //Same bib record
                            case { it.bib_id == currentRecordId }:
                                print "."
                                previousAuthData << currentAuthData
                                break
                        //New record
                            default:
                                print "| "
                                // task {
                                handleRow(previousBibResultSet, collection, previousAuthData)
                                //}
                                previousBibResultSet = rowMap
                                previousAuthData = []
                                previousAuthData << [currentAuthData]
                        }
                        break
                }
            }
            //Last row
            handleRow(previousBibResultSet, collection, previousAuthData)

        }

        catch (any) {
            println any.message
            throw any
        }
        finally {
            s_mainTableWriter.close()
            s_identifiersWriter.close()
        }
        //}

        def endSecs = (System.currentTimeMillis() - startTime) / 1000
        println "Done. Processed  ${counter} documents in ${endSecs} seconds."

    }

    static Map composeAuthData(LinkedHashMap<String, Object> map) {
        def fields = map.data.fields
        if (fields."100" != null && fields."100".subfields.t.any { it -> it.any { tit -> tit } }) {
            [type: 'title', value: fields."100".subfields.t.first().first()]
        } else if (fields."100" != null && fields."100".subfields.a.any { it -> it.any { tit -> tit } }) {
            [field: 'personalName', value: fields."100".subfields.a.first().first()]
        } else {
            print 'Unhandled authority record'
            throw new Exception("Unhandled authority record")
            [type: 'unknown', value: null]
        }


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

    private
    static void handleRow(Map rowMap, String collection, List setSpecs) {

        Map doc = getMarcDocMap(rowMap.data as byte[])
        List
        if (doc) {

            if (collection == 'bib' && setSpecs.count { it } > 0) {

                doc.fields.each { bibField ->
                    String key = bibField.keySet()[0]
                    switch (key) {
                        case { a -> ['100', '700'].contains(a) }:
                            def authRecord = setSpecs.find { it -> it.field == 'personalName' && getSubfieldValue(bibField[key], "a") == it.value }
                            println authRecord ? "Match! on ${authRecord.id}" : "_"
                            break
                        default:
                            break
                    }
                }

                /* setSpecs.each { it ->
                     println it.value
                     if (it.type == "personalName") {
                         if (doc.fields.'100'.subfields.a.first().first() == it.value) {
                             println "match!"
                         }
                         if(doc.fields.'700'.subfields.a.first().first() == it.value){
                             println "match!"
                         }
                     }
                 }*/
            }
            if (!isSuppressed(doc)) {
                String oldStyleIdentifier = "/" + collection + "/" + getControlNumber(doc)
                def id = LegacyIntegrationTools.generateId(oldStyleIdentifier)
                Map convertedData = setSpecs && setSpecs.size() > 1 && collection != 'bib' ?
                        s_marcFrameConverter.convert(doc, id, [oaipmhSetSpecs: setSpecs]) :
                        s_marcFrameConverter.convert(doc, id)
                Document document = new Document(convertedData)
                document.created = rowMap.created
                writeDocumentToLoadFile(document, collection)
            }
        }
    }

    static String getSubfieldValue(Map p, String s) {

        for (subfield in p.subfields) {
            String key = subfield.keySet()[0];
            if (key == s) {
                return subfield[key]
            }
        }
        return null
    }

    static List getHoldOaipmhSetSpecs(def resultSet) {
        List specs = []
        /* if (collection == "bib") {
             int authId = resultSet.auth_id ?: 0
             if (authId > 0) {
                 specs.add("authority:${authId}")
             }
         } else if (collection == "hold") {*/
        if (resultSet.bib_id > 0)
            specs.add("bibid:${resultSet.bib_id}")
        if (resultSet.sigel)
            specs.add("location:${resultSet.sigel}")
        // }
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
                            return true;
                    }
                }
            }
        }
        return false;
    }

    private static String getControlNumber(Map doc) {
        def fields = doc.get("fields")
        for (def field : fields) {
            if (field.get("001") != null)
                return field.get("001");
        }
        return null
    }

    private static
    synchronized void writeDocumentToLoadFile(Document doc, String collection) {
        /* columns:

           id text not null unique primary key,
           data jsonb not null,
           collection text not null,
           changedIn text not null,
           changedBy text,
           checksum text not null,
           created timestamp with time zone not null default now(),
           modified timestamp with time zone not null default now(),
           deleted boolean default false

           */

        final char delimiter = '\t';
        final String nullString = "\\N";

        final delimiterString = new String(delimiter);

        List<String> identifiers = doc.recordIdentifiers;
        print "w"
        // Write to main table file

        s_mainTableWriter.write(doc.shortId);
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write(doc.dataAsString.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString));
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write(collection.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString));
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write("vcopy");
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write(nullString);
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write(doc.checksum.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString));
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write(doc.created);

        // remaining values have sufficient defaults.

        s_mainTableWriter.newLine();

        // Write to identifiers table file

        /* columns:
        id text not null,
        identifier text not null -- unique
        */

        for (String identifier : identifiers) {
            s_identifiersWriter.write(doc.shortId);
            s_identifiersWriter.write(delimiter);
            s_identifiersWriter.write(identifier);

            s_identifiersWriter.newLine();
        }
    }

}
