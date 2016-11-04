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

//import groovyx.gpars.GParsPool
//import groovyx.gpars.ParallelEnhancer
/**
 * Writes documents into a PostgreSQL load-file, which can be efficiently imported into lddb
 */
@Log
class PostgresLoadfileWriter {
    private static
    final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final int CONVERSIONS_PER_THREAD = 100;

    // USED FOR DEV ONLY, MUST _NEVER_ BE SET TO TRUE ONCE XL GOES INTO PRODUCTION. WITH THIS SETTING THE IMPORT WILL
    // _SKIP_ DOCUMENTS THAT FAIL CONVERSION, RESULTING IN POTENTIAL DATA LOSS IF USED WHEN IMPORTING TO A PRODUCTION XL
    private static final boolean FAULT_TOLERANT_MODE = true;

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

        try {
            def sql = Sql.newInstance(connectionUrl, "com.mysql.jdbc.Driver")
            sql.withStatement { stmt -> stmt.fetchSize = Integer.MIN_VALUE }
            sql.connection.setAutoCommit(false)
            sql.setResultSetType(ResultSet.TYPE_FORWARD_ONLY)
            sql.setResultSetConcurrency(ResultSet.CONCUR_READ_ONLY)

            int previousRecordId = -1
            List previousSpecs = []
            Map previousBibResultSet = null



            sql.eachRow(MySQLLoader.selectByMarcType[collection], [0]) { ResultSet currentRow ->

                /* if (++counter % 1000 == 0) {
                     def elapsedSecs = (System.currentTimeMillis() - startTime) / 1000
                     if (elapsedSecs > 0) {
                         def docsPerSec = counter / elapsedSecs
                         println "Working. Currently ${counter} documents saved. Crunching ${docsPerSec} docs / s"
                     }
                 }*/
                //bib.bib_id, bib.data, bib.create_date, auth.auth_id
                try {
                    int currentRecordId = currentRow.getInt(1)


                    if (collection == 'bib' && (previousBibResultSet.bib_id == -1)) {
                        println "1"
                        //Första varvet
                        previousBibResultSet = [bib_id: currentRow.bib_id, data: currentRow.data, create_date: currentRow.create_date, auth_id: currentRow.auth_id]
                        //TODO: make closure out of this
                        previousRecordId = currentRecordId
                    } else if (collection == 'bib' && previousBibResultSet.bib_id == currentRecordId) {
                        print "."
                        //Flera poster med samma ID
                        previousSpecs.addAll(getOaipmhSetSpecs(previousBibResultSet, collection))

                        previousBibResultSet = [bib_id: currentRow.bib_id, data: currentRow.data, create_date: currentRow.create_date, auth_id: currentRow.auth_id]
                    } else {
                        //Poster med olika id. Antingen sista av flera eller en ny post
                        print "|"
                        previousSpecs.addAll(getOaipmhSetSpecs(previousBibResultSet, collection))

                        //handleRow(previousRow,collection)

                        //Reset stuff for next record Id
                        def authposts = previousSpecs.findAll { String it ->
                            it.startsWith("auth")
                        }.count { it }
                        if (authposts > 1)
                            print "${authposts}"
                        previousBibResultSet = [bib_id: currentRow.bib_id, data: currentRow.data, create_date: currentRow.create_date, auth_id: currentRow.auth_id]
                        previousSpecs = getOaipmhSetSpecs(currentRow, collection)
                    }

                    //TODO: what about other collections!

                } catch (any) {
                    println all.message
                    println all.stackTrace
                }

            }
            //Last row
            previousSpecs << getOaipmhSetSpecs(previousBibResultSet, collection)
            handleRow(previousBibResultSet, collection)

        }
        catch (all) {
            println all.message
        }
        finally {
            s_mainTableWriter.close()
            s_identifiersWriter.close()
        }

        def endSecs = (System.currentTimeMillis() - startTime) / 1000
        println "  Done. Processed ${counter} documents in ${endSecs} seconds."

    }

    private static void handleRow(def row, String collection) {
        MarcRecord record = Iso2709Deserializer.deserialize(
                MySQLLoader.normalizeString(
                        new String(row.getBytes("data"), "UTF-8"))
                        .getBytes("UTF-8"))
        if (record) {
            Map doc = MarcJSONConverter.toJSONMap(record)
            if (doc) {
                if (!isSuppressed(doc))
                    handleDM(toDocumentMap(doc, row, collection), s_marcFrameConverter)
            }
        }
    }

    private static Map toDocumentMap(Map doc, def row, String collection) {
        String oldStyleIdentifier = "/" + collection + "/" + getControlNumber(doc)
        String id = LegacyIntegrationTools.generateId(oldStyleIdentifier)
        [record: doc, collection: collection, id: id, created: row.getTimestamp("create_date")]
    }

    static List getOaipmhSetSpecs(def resultSet, String collection) {
        List specs = []
        if (collection == "bib") {
            int authId = resultSet.auth_id
            if (authId > 0)
                specs.add("authority:" + authId + "DEBUG: ${resultSet.bib_id}")

        } else if (collection == "hold") {
            int bibId = resultSet.getInt("bib_id")
            String sigel = resultSet.getString("shortname")
            if (bibId > 0)
                specs.add("bibid:" + bibId)
            if (sigel)
                specs.add("location:" + sigel)
        }
        return specs
    }


    private
    static String getShortId(Map documentMap, MarcFrameConverter marcFrameConverter) {
        try {
            Map convertedData = marcFrameConverter.convert(documentMap.record, documentMap.id);
            Document document = new Document(convertedData)
            document.setCreated(documentMap.created)
            return document.getShortId()

        } catch (Throwable e) {
            e.print("Convert Failed. id: ${documentMap.id}")
            e.printStackTrace()
            //String voyagerId = dm.collection + "/" + getControlNumber(dm.record);
            //s_failedIds.add(voyagerId);
        }
    }

    private
    static void handleDM(Map documentMap, MarcFrameConverter marcFrameConverter) {
        try {
            Map convertedData = marcFrameConverter.convert(documentMap.record, documentMap.id);
            Document document = new Document(convertedData)
            document.setCreated(documentMap.created)
            writeDocumentToLoadFile(document, documentMap.collection)

        } catch (Throwable e) {
            e.println("Convert Failed. id: ${documentMap.id}")
            e.printStackTrace()
        }
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

        List<String> identifiers = doc.getRecordIdentifiers();

        // Write to main table file

        s_mainTableWriter.write(doc.getShortId());
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write(doc.getDataAsString().replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString));
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write(collection.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString));
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write("vcopy");
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write(nullString);
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write(doc.getChecksum().replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString));
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write(doc.getCreated());

        // remaining values have sufficient defaults.

        s_mainTableWriter.newLine();

        // Write to identifiers table file

        /* columns:
        id text not null,
        identifier text not null -- unique
        */

        for (String identifier : identifiers) {
            s_identifiersWriter.write(doc.getShortId());
            s_identifiersWriter.write(delimiter);
            s_identifiersWriter.write(identifier);

            s_identifiersWriter.newLine();
        }
    }

}
