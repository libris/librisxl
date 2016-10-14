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

//import groovyx.gpars.GParsPool
//import groovyx.gpars.ParallelEnhancer
import java.nio.file.Paths
import java.sql.ResultSet

/**
 * Writes documents into a PostgreSQL load-file, which can be efficiently imported into lddb
 */
@Log
class PostgresLoadfileWriter {
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
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

    public static void dumpStraight(String exportFileName, String collection, String connectionUrl) {
        s_marcFrameConverter = new MarcFrameConverter();
        s_mainTableWriter = Files.newBufferedWriter(Paths.get(exportFileName), Charset.forName("UTF-8"));
        s_identifiersWriter = Files.newBufferedWriter(Paths.get(exportFileName + "_identifiers"), Charset.forName("UTF-8"));
        def loader = new MySQLLoader(connectionUrl, collection);

        def counter = 0
        def startTime = System.currentTimeMillis()

        try {

            loader.run { doc, specs, createDate ->


                if (isSuppressed(doc))
                    return

                String oldStyleIdentifier = "/" + collection + "/" + getControlNumber(doc)
                String id = LegacyIntegrationTools.generateId(oldStyleIdentifier)

                Map documentMap = [record: doc, collection: collection, id: id, created: createDate]

                handleDM(documentMap, collection)

                if (++counter % 2000 == 0) {
                    def elapsedSecs = (System.currentTimeMillis() - startTime) / 1000
                    if (elapsedSecs > 0) {
                        def docsPerSec = counter / elapsedSecs
                        println "Working. Currently $counter documents saved. Crunching $docsPerSec docs / s"
                    }
                }

            }

        } finally {
            s_mainTableWriter.close()
            s_identifiersWriter.close()
        }

        def endSecs = (System.currentTimeMillis() - startTime) / 1000
        println "Done. Processed $counter documents in $endSecs seconds."

    }


    public static void dumpGpars(String exportFileName, String collection, String connectionUrl) {

        Vector m_outputQueue = new Vector(CONVERSIONS_PER_THREAD);

        if (FAULT_TOLERANT_MODE)
            System.out.println("\t**** RUNNING IN FAULT TOLERANT MODE, DOCUMENTS THAT FAIL CONVERSION WILL BE SKIPPED.\n" +
                    "\tIF YOU ARE IMPORTING TO A PRODUCTION XL, ABORT NOW!! AND RECOMPILE WITH FAULT_TOLERANT_MODE=false");

        s_marcFrameConverter = new MarcFrameConverter();
        s_mainTableWriter = Files.newBufferedWriter(Paths.get(exportFileName), Charset.forName("UTF-8"));
        s_identifiersWriter = Files.newBufferedWriter(Paths.get(exportFileName + "_identifiers"), Charset.forName("UTF-8"));
        //def loader = new MySQLLoader(connectionUrl, collection);

        def counter = 0
        def startTime = System.currentTimeMillis()

        try {
            //loader.run { doc, specs, createDate ->
            //Connection connection = DriverManager.getConnection(connectionUrl)
            def sql = Sql.newInstance(connectionUrl,"com.mysql.jdbc.Driver")
            sql.withStatement { stmt -> stmt.fetchSize = Integer.MIN_VALUE }
            sql.connection.setAutoCommit(false)
            sql.setResultSetType(ResultSet.TYPE_FORWARD_ONLY)
            sql.setResultSetConcurrency(ResultSet.CONCUR_READ_ONLY)

            sql.eachRow(MySQLLoader.selectByMarcType[collection], [0]) { dataRow ->
                m_outputQueue.add(dataRow)
                if (m_outputQueue.size() >= CONVERSIONS_PER_THREAD) {
                    // GParsPool.withPool {
                    List specs = null
                    m_outputQueue.collate(25).each { coll ->
                        def m = new MarcFrameConverter()

                        coll.each {  row ->
                            try{
                            int currentRecordId = -1
                            Map doc = null
                            int recordId = row.getInt(1)
                            MarcRecord record = Iso2709Deserializer.deserialize(
                                    MySQLLoader.normalizeString(
                                            new String(row.getBytes("data"), "UTF-8")).getBytes("UTF-8"))
                            if (record) {
                                doc = MarcJSONConverter.toJSONMap(record)
                                if (!recordId.equals(currentRecordId)) {
                                    specs = MySQLLoader.getOaipmhSetSpecs(row, collection)
                                    if (doc) {
                                        if (isSuppressed(doc))
                                            return

                                        String oldStyleIdentifier = "/" + collection + "/" + getControlNumber(doc)
                                        String id = LegacyIntegrationTools.generateId(oldStyleIdentifier)
                                        Map documentMap = [record: doc, collection: collection, id: id, created: row.getTimestamp("create_date")]
                                        handleDM(documentMap, m)
                                    }
                                    currentRecordId = recordId
                                    doc = [:]
                                    specs = []
                                }
                            }
                            specs = MySQLLoader.getOaipmhSetSpecs(row, collection)
                            if (doc) {
                                if (isSuppressed(doc))
                                    return

                                String oldStyleIdentifier = "/" + collection + "/" + getControlNumber(doc)
                                String id = LegacyIntegrationTools.generateId(oldStyleIdentifier)
                                Map documentMap = [record: doc, collection: collection, id: id, created: row.getTimestamp("create_date")]
                                handleDM(documentMap, m)
                            }
                        }catch(all){
                                println all.message
                            }
                        }
                    }
                    // }

                    m_outputQueue = new Vector<HashMap>(CONVERSIONS_PER_THREAD);
                }

                if (++counter % 100 == 0) {
                    def elapsedSecs = (System.currentTimeMillis() - startTime) / 1000
                    if (elapsedSecs > 0) {
                        def docsPerSec = counter / elapsedSecs
                        println "Working. Currently ${counter} documents saved. Crunching ${docsPerSec} docs / s"
                    }
                }
            }



            if (!m_outputQueue.isEmpty())
                println "last_one"

        }

        finally {
            s_mainTableWriter.close()
            s_identifiersWriter.close()
        }

        def endSecs = (System.currentTimeMillis() - startTime) / 1000
        println " Done. Processed  ${counter}  documents in  ${endSecs}  seconds. "

    }

    private static void handleDM(Map documentMap, MarcFrameConverter marcFrameConverter) {
        try {
            Map convertedData = marcFrameConverter.convert(documentMap.record, documentMap.id);
            Document document = new Document(convertedData)
            document.setCreated(documentMap.created)
            writeDocumentToLoadFile(document, documentMap.collection)

        } catch (Throwable e) {
            e.print("Convert Failed. id: ${documentMap.id}")
            e.printStackTrace()
            //String voyagerId = dm.collection + "/" + getControlNumber(dm.record);
            //s_failedIds.add(voyagerId);
        }
    }

    public static void dump(String exportFileName, String collection, String connectionUrl) {
        Vector<HashMap> m_outputQueue = new Vector<HashMap>(CONVERSIONS_PER_THREAD);

        if (FAULT_TOLERANT_MODE)
            System.out.println("\t**** RUNNING IN FAULT TOLERANT MODE, DOCUMENTS THAT FAIL CONVERSION WILL BE SKIPPED.\n" +
                    "\tIF YOU ARE IMPORTING TO A PRODUCTION XL, ABORT NOW!! AND RECOMPILE WITH FAULT_TOLERANT_MODE=false");

        s_marcFrameConverter = new MarcFrameConverter();
        s_mainTableWriter = Files.newBufferedWriter(Paths.get(exportFileName), Charset.forName("UTF-8"));
        s_identifiersWriter = Files.newBufferedWriter(Paths.get(exportFileName + "_identifiers"), Charset.forName("UTF-8"));
        s_threadPool = new Thread[THREAD_COUNT];

        def loader = new MySQLLoader(connectionUrl, collection);

        def counter = 0
        def startTime = System.currentTimeMillis()

        try {
            loader.run { doc, specs, createDate ->

                if (isSuppressed(doc))
                    return

                String oldStyleIdentifier = "/" + collection + "/" + getControlNumber(doc)
                String id = LegacyIntegrationTools.generateId(oldStyleIdentifier)


                Map documentMap = new HashMap(2)
                documentMap.put("record", doc)
                documentMap.put("collection", collection)
                documentMap.put("id", id)
                documentMap.put("created", createDate)
                m_outputQueue.add(documentMap);

                if (m_outputQueue.size() >= CONVERSIONS_PER_THREAD) {
                    flushOutputQueue(m_outputQueue);
                    m_outputQueue = new Vector<HashMap>(CONVERSIONS_PER_THREAD);
                }

                if (++counter % 1000 == 0) {
                    def elapsedSecs = (System.currentTimeMillis() - startTime) / 1000
                    if (elapsedSecs > 0) {
                        def docsPerSec = counter / elapsedSecs
                        println "Working. Currently $counter documents saved. Crunching $docsPerSec docs / s"
                    }
                }
            }

            if (!m_outputQueue.isEmpty())
                flushOutputQueue(m_outputQueue);

        } finally {
            for (int i = 0; i < THREAD_COUNT; ++i) {
                if (s_threadPool[i] != null)
                    s_threadPool[i].join();
            }
            s_mainTableWriter.close()
            s_identifiersWriter.close()
        }

        def endSecs = (System.currentTimeMillis() - startTime) / 1000
        println "Done. Processed $counter documents in $endSecs seconds."
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

/*
private static void addSetSpecs(Map manifest, List specs)
{
    if (specs.size() == 0)
        return

    def extradata = manifest.get("extraData", [:])
    def setSpecs = extradata.get("oaipmhSetSpecs", [])

    for (String spec : specs)
    {
        setSpecs.add(spec);
    }
}
*/

    private static void flushOutputQueue(Vector<HashMap> threadWorkLoad) {
        // Find a suitable thread from the pool to do the conversion

        int i = 0;
        while (true) {
            i++;
            if (i == THREAD_COUNT) {
                i = 0;
                Thread.yield();
            }

            if (s_threadPool[i] == null || s_threadPool[i].state == Thread.State.TERMINATED) {
                s_threadPool[i] = new Thread(new Runnable()
                {
                    void run() {
                        for (HashMap dm : threadWorkLoad) {
                            if (FAULT_TOLERANT_MODE) {
                                try {
                                    Map convertedData = s_marcFrameConverter.convert(dm.record, dm.id);
                                    Document doc = new Document(convertedData)
                                    doc.setCreated(dm.created)

                                    writeDocumentToLoadFile(doc, dm.collection);
                                } catch (Throwable e) {
                                    e.print("Convert Failed. id: ${dm.id}")
                                    e.printStackTrace()
                                    String voyagerId = dm.collection + "/" + getControlNumber(dm.record);
                                    s_failedIds.add(voyagerId);
                                }
                            } else {
                                Map convertedData = s_marcFrameConverter.convert(dm.record, dm.id);
                                Document doc = new Document(convertedData)
                                doc.setCreated(dm.created)

                                writeDocumentToLoadFile(doc, dm.collection);
                            }
                        }
                    }
                });
                s_threadPool[i].start();
                return;
            }
        }
    }

    private static synchronized void writeDocumentToLoadFile(Document doc, String collection) {
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
