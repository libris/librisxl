package whelk.tools

import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.Iso2709Deserializer
import whelk.Document
import whelk.converter.MarcJSONConverter
import whelk.converter.marc.MarcFrameConverter
import whelk.importer.MySQLImporter
import whelk.util.LegacyIntegrationTools
import whelk.util.PropertyLoader
import groovy.util.logging.Slf4j as Log

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet

/**
 * Writes documents into a PostgreSQL load-file, which can be efficiently imported into lddb
 */
@Log
class PostgresLoadfileWriter
{
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver"
    private static final int THREAD_COUNT = 8;
    private static final int CONVERSIONS_PER_THREAD = 200;

    private final String m_exportFileName;
    private final String m_collection;
    private final Connection m_connection
    private final PreparedStatement m_statement;
    private final ResultSet m_resultSet;
    private final MarcFrameConverter m_marcFrameConverter;
    private final BufferedWriter m_mainTableWriter;
    private final BufferedWriter m_identifiersWriter;
    private final Thread[] m_threadPool;
    private Vector<HashMap> m_workingSet = new Vector<HashMap>(CONVERSIONS_PER_THREAD);

    public PostgresLoadfileWriter(String exportFileName, String collection)
    {
        Class.forName(JDBC_DRIVER);

        Properties props = PropertyLoader.loadProperties("mysql");
        m_connection = DriverManager.getConnection(props.getProperty("mysqlConnectionUrl"));
        m_connection.setAutoCommit(false);

        switch (collection)
        {
            case "auth":
                m_statement = m_connection.prepareStatement("SELECT auth_id, data FROM auth_record WHERE auth_id > ? AND deleted = 0 ORDER BY auth_id", java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)
                break;
            case "bib":
                m_statement = m_connection.prepareStatement("SELECT bib.bib_id, bib.data, auth.auth_id FROM bib_record bib LEFT JOIN auth_bib auth ON bib.bib_id = auth.bib_id WHERE bib.bib_id > ? AND bib.deleted = 0 ORDER BY bib.bib_id", java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)
                break;
            case "hold":
                m_statement = m_connection.prepareStatement("SELECT mfhd_id, data, bib_id, shortname FROM mfhd_record WHERE mfhd_id > ? AND deleted = 0 ORDER BY mfhd_id", java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)
                break;
            default:
                throw new Exception("No valid collection selected");
        }
        m_statement.setFetchSize(Integer.MIN_VALUE)
        m_statement.setInt(1, 0); // start from id 0
        m_resultSet = m_statement.executeQuery();
        m_exportFileName = exportFileName;
        m_collection = collection;
        m_marcFrameConverter = new MarcFrameConverter();
        m_mainTableWriter = Files.newBufferedWriter(Paths.get(exportFileName), Charset.forName("UTF-8"));
        m_identifiersWriter = Files.newBufferedWriter(Paths.get(exportFileName+"_identifiers"), Charset.forName("UTF-8"));
        m_threadPool = new Thread[THREAD_COUNT];
    }

    public void generatePostgresLoadFile()
    {
        long startTime = System.currentTimeMillis();
        int savedDocumentsCount = 0;

        // The document we're currently building. May contain several ResultSet rows
        String currentDocumentId = "";
        HashMap documentMap = null;

        while (m_resultSet.next())
        {
            //int recordId = m_resultSet.getInt(1);
            MarcRecord record = Iso2709Deserializer.deserialize(
                    MySQLImporter.normalizeString(new String(m_resultSet.getBytes("data"), "UTF-8")).getBytes("UTF-8"));

            if (record)
            {
                def field001List = record.getControlfields("001");
                if (field001List.size() == 0)
                    continue; // skip document if no 001 control field

                def aList = record.getDatafields("599").collect { it.getSubfields("a").data }.flatten()
                if ("SUPPRESSRECORD" in aList)
                    continue; // skip document if SUPRESSED

                String oldStyleIdentifier = "/"+m_collection+"/"+field001List.get(0).getData()
                String id = LegacyIntegrationTools.generateId(oldStyleIdentifier)

                if (!id.equals(currentDocumentId))
                {
                    // New ID, store current document and begin constructing a new one
                    if (documentMap)
                    {
                        appendToOutputQueue(documentMap, false)

                        if (++savedDocumentsCount % 1000 == 0)
                        {
                            long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
                            if (elapsedSeconds > 0)
                            {
                                long documentsPerSecond = savedDocumentsCount / elapsedSeconds;
                                System.out.println("Working. Currently " + savedDocumentsCount +
                                        " documents saved. Crunching " + documentsPerSecond + " docs / s");
                            }
                        }
                    }
                    currentDocumentId = id;
                    documentMap = new HashMap();
                }

                documentMap.put("record", record)
                documentMap.put("manifest", [(Document.ID_KEY):currentDocumentId,(Document.COLLECTION_KEY):m_collection, (Document.ALTERNATE_ID_KEY): [oldStyleIdentifier]])
            }

            addOaipmhSetSpecs(documentMap, m_resultSet);
        }

        // Don't forget about the last document being constructed. Must be written as well.
        if ( ! (new HashMap().equals(documentMap)) )
        {
            appendToOutputQueue(documentMap, true)
            ++savedDocumentsCount;
        }

        cleanup();

        long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
        System.out.println("Done. Saved " + savedDocumentsCount + " documents in " + elapsedSeconds + " seconds.");
    }

    private void addOaipmhSetSpecs(HashMap documentMap, ResultSet resultSet)
    {
        switch (m_collection)
        {
            case "bib":
                int auth_id = resultSet.getInt("auth_id")
                if (auth_id > 0)
                    documentMap.get("manifest", [:]).get("extraData", [:]).get("oaipmhSetSpecs", []).add("authority:" + auth_id)
                break;
            case "hold":
                int bib_id = resultSet.getInt("bib_id")
                String sigel = resultSet.getString("shortname")
                if (bib_id > 0)
                    documentMap.get("manifest", [:]).get("extraData", [:]).get("oaipmhSetSpecs", []).add("bibid:" + bib_id)
                if (sigel)
                    documentMap.get("manifest", [:]).get("extraData", [:]).get("oaipmhSetSpecs", []).add("location:" + sigel)
                break;
        }
    }

    private void appendToOutputQueue(HashMap documentMap, boolean flush)
    {
        m_workingSet.add(documentMap);
        if (m_workingSet.size() < CONVERSIONS_PER_THREAD && !flush)
            return;

        Vector<HashMap> threadWorkLoad = m_workingSet;
        m_workingSet = new Vector<HashMap>(CONVERSIONS_PER_THREAD);

        // Find a suitable thread from the pool to do the conversion

        int i = 0;
        while(true)
        {
            i++;
            if (i == THREAD_COUNT)
                i = 0;

            if (m_threadPool[i] == null || m_threadPool[i].state == Thread.State.TERMINATED)
            {
                m_threadPool[i] = new Thread(new Runnable()
                {
                    void run()
                    {
                        for (HashMap dm : threadWorkLoad)
                        {
                            dm.manifest[Document.CHANGED_IN_KEY] = "vcopy";
                            Document doc = new Document(MarcJSONConverter.toJSONMap(dm.record), dm.manifest);
                            doc = m_marcFrameConverter.convert(doc);
                            writeDocumentToLoadFile(doc);
                        }
                    }
                });
                m_threadPool[i].start();
                return;
            }

            Thread.yield()
        }
    }

    private synchronized void writeDocumentToLoadFile(Document doc)
    {
        /* columns:
        id text not null unique primary key,
        data jsonb not null,
        manifest jsonb not null,
        quoted jsonb,
        created timestamp with time zone not null default now(),
        modified timestamp with time zone not null default now(),
        deleted boolean default false*/

        final char delimiter = '\t';
        final String nullString = "\\N";

        final delimiterString = new String(delimiter);

        // Write to main table file

        m_mainTableWriter.write(doc.getId());
        m_mainTableWriter.write(delimiter);
        m_mainTableWriter.write( doc.getDataAsString().replace("\\", "\\\\").replace(delimiterString, "\\"+delimiterString) );
        m_mainTableWriter.write(delimiter);
        m_mainTableWriter.write( doc.getManifestAsJson().replace("\\", "\\\\").replace(delimiterString, "\\"+delimiterString) );
        m_mainTableWriter.write(delimiter);
        String quoted = doc.getQuotedAsString();
        if (quoted)
            m_mainTableWriter.write(quoted.replace("\\", "\\\\").replace(delimiterString, "\\"+delimiterString));
        else
            m_mainTableWriter.write(nullString);

        // remaining values have defaults.

        m_mainTableWriter.newLine();

        // Write to identifiers table file

        doc.findIdentifiers()
        List<String> identifiers = doc.getIdentifiers();

        /* columns:
        id text not null,
        identifier text not null -- unique
        */

        for (String identifier : identifiers)
        {
            m_identifiersWriter.write(doc.getId());
            m_identifiersWriter.write(delimiter);
            m_identifiersWriter.write(identifier);

            m_identifiersWriter.newLine();
        }
    }

    private void cleanup()
    {
        for (int i = 0; i < THREAD_COUNT; ++i)
        {
            if (m_threadPool[i] != null)
                m_threadPool[i].join();
        }

        m_identifiersWriter.close();
        m_mainTableWriter.close();

        m_resultSet.close();
        m_statement.close();
        m_connection.close();
    }
}
