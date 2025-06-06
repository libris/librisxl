package whelk.export.marc;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import se.kb.libris.export.ExportProfile;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.Iso2709MarcRecordWriter;
import se.kb.libris.util.marc.io.MarcRecordWriter;
import se.kb.libris.util.marc.io.MarcXmlRecordReader;
import se.kb.libris.util.marc.io.MarcXmlRecordWriter;
import whelk.Document;
import whelk.JsonLd;
import whelk.MarcExportServer;
import whelk.Whelk;
import whelk.XlServer;
import whelk.converter.marc.JsonLD2MarcXMLConverter;
import whelk.util.BlockingThreadPool;
import whelk.util.BlockingThreadPool.SimplePool;
import whelk.util.MarcExport;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

public class MarcCliExport
{
    private final int BATCH_SIZE = 200;
    private final JsonLD2MarcXMLConverter m_toMarcXmlConverter;
    private final Whelk m_whelk;
    private final Set<String> exportedUris = new TreeSet<>();

    final static Logger log = LogManager.getLogger(MarcCliExport.class);

    public MarcCliExport(Whelk whelk)
    {
        m_whelk = whelk;
        m_toMarcXmlConverter = new JsonLD2MarcXMLConverter(whelk.getMarcFrameConverter());
    }

    class Batch
    {
        public Batch(ExportProfile profile, MarcRecordWriter output)
        {
            bibUrisToConvert = new ArrayList<>(BATCH_SIZE);
            this.profile = profile;
            this.output = output;
        }
        List<String> bibUrisToConvert;
        ExportProfile profile;
        MarcRecordWriter output;
    }

    public static void main(String[] args)
            throws Exception {
        if (args.length != 2 && args.length != 1)
            printUsageAndExit();

        if (args[0].equals("--server"))
        {
            new MarcExportServer().run();
            return;
        }

        if (args[0].equals("--sao"))
        {
            MarcRecordWriter output = new MarcXmlRecordWriter(System.out, "UTF-8");
            new MarcCliExport(Whelk.createLoadedCoreWhelk()).dumpSao(output);
            output.close();
            return;
        }

        if (args[0].equals("--auth"))
        {
            MarcRecordWriter output = new Iso2709MarcRecordWriter(System.out, "UTF-8");
            new MarcCliExport(Whelk.createLoadedCoreWhelk()).dumpAuth(output);
            output.close();
            return;
        }

        ExportProfile profile = new ExportProfile(new File(args[0]));
        String encoding = profile.getProperty("characterencoding");
        if (encoding.equals("Latin1Strip")) {
            encoding = "ISO-8859-1";
        }
        MarcRecordWriter output = null;
        if (profile.getProperty("format", "ISO2709").equalsIgnoreCase("MARCXML"))
            output = new MarcXmlRecordWriter(System.out, encoding);
        else
            output = new Iso2709MarcRecordWriter(System.out, encoding);

        if (args.length == 2)
        {
            Path idFilePath = new File(args[1]).toPath();
            new MarcCliExport(Whelk.createLoadedCoreWhelk()).dumpSpecific(profile, idFilePath, output);
        } else if (args.length == 1)
        {
            // If the "profile" contains start/stop times, we should generate an interval-export instead
            // of a total export.
            String start = profile.getProperty("start");
            String stop = profile.getProperty("stop");
            if (start != null && stop != null)
            {
                start = start +"T00:00:00Z";
                stop = stop +"T23:59:59Z";
                var parameters = new ProfileExport.Parameters(profile, start, stop, ProfileExport.DELETE_MODE.IGNORE, false);
                Whelk whelk = Whelk.createLoadedCoreWhelk();
                ProfileExport pf = new ProfileExport(whelk, whelk.getStorage().createAdditionalConnectionPool("ProfileExport"));
                pf.exportInto(output, parameters);
                pf.shutdown();
            }
            else
                new MarcCliExport(Whelk.createLoadedCoreWhelk()).dump(profile, output);
        }
        output.close();
    }

    private static void printUsageAndExit()
    {
        System.out.println("Usage:");
        System.out.println();
        System.out.println("  To export a collection of IDs:");
        System.out.println("  java -Dxl.secret.properties=SECRETPROPSFILE -jar marc_export.jar PROFILE-FILE ID-FILE");
        System.out.println();
        System.out.println("  To do a \"complete\" or \"interval\" export, for a given profile");
        System.out.println("  (the profile must contain start/stop to make it an interval export):");
        System.out.println("  java -Dxl.secret.properties=SECRETPROPSFILE -jar marc_export.jar PROFILE-FILE");
        System.out.println();
        System.out.println("   PROFILE-FILE should be a Java-properties file with the export-profile settings.");
        System.out.println("   ID-FILE should be a file with IDs to export, containing one URI per row.");
        System.out.println();
        System.out.println("  To do an SAO export:");
        System.out.println("  java -Dxl.secret.properties=SECRETPROPSFILE -jar marc_export.jar --sao");
        System.out.println();
        System.out.println("  To do an AUTH export for regina/aleph:");
        System.out.println("  java -Dxl.secret.properties=SECRETPROPSFILE -jar marc_export.jar --auth");
        System.out.println();
        System.out.println("For example:");
        System.out.println(" java -jar marc_export.jar export.properties");
        System.out.println("Would export all records held by whatever is in location=[] in export.properties.");
        System.exit(1);
    }

    private void dumpSao(MarcRecordWriter output) throws SQLException, IOException
    {
        Set<String> agentTypes = m_whelk.getJsonld().getSubClasses("Agent");
        agentTypes.add("Agent");
        try(Connection connection = m_whelk.getStorage().getOuterConnection();
            PreparedStatement preparedStatement = getAllSaoAndAgentsStatement(connection, agentTypes);
            ResultSet resultSet = preparedStatement.executeQuery())
        {
            while (resultSet.next())
            {
                String id = resultSet.getString("id");
                Document doc = m_whelk.loadEmbellished(id);

                String marcXml = null;
                try
                {
                    marcXml = (String) m_toMarcXmlConverter.convert(doc.data, doc.getShortId()).get(JsonLd.NON_JSON_CONTENT_KEY);
                }
                catch (Exception | Error e)
                { // Depending on the converter, a variety of problems may arise here
                    log.error("Conversion error for: " + doc.getCompleteId() + " cause: ", e);
                    continue;
                }

                MarcRecord marcRecord = MarcXmlRecordReader.fromXml(marcXml);
                output.writeRecord(marcRecord);
            }
        }
    }

    private void dumpAuth(MarcRecordWriter output) throws SQLException, IOException
    {
        HashSet<String> workDerivativeTypes = new HashSet<>(m_whelk.getJsonld().getSubClasses("Work"));

        try(Connection connection = m_whelk.getStorage().getOuterConnection();
            PreparedStatement preparedStatement = getAllAuthStatement(connection);
            ResultSet resultSet = preparedStatement.executeQuery())
        {
            while (resultSet.next())
            {
                String id = resultSet.getString("id");
                Document doc = m_whelk.loadEmbellished(id);

                // We want to only dump traditional MARC auth records. No work-records whatsoever.
                String mainEntityType = doc.getThingType();
                if ( workDerivativeTypes.contains(mainEntityType) || mainEntityType.equals("Work") )
                    continue;

                String marcXml = null;
                try
                {
                    marcXml = (String) m_toMarcXmlConverter.convert(doc.data, doc.getShortId()).get(JsonLd.NON_JSON_CONTENT_KEY);
                }
                catch (Exception | Error e)
                { // Depending on the converter, a variety of problems may arise here
                    log.error("Conversion error for: " + doc.getCompleteId() + " cause: ", e);
                    continue;
                }

                MarcRecord marcRecord = MarcXmlRecordReader.fromXml(marcXml);
                output.writeRecord(marcRecord);
            }
        }
    }

    private void dump(ExportProfile profile, MarcRecordWriter output)
            throws SQLException, InterruptedException
    {
        SimplePool threadPool = BlockingThreadPool.simplePool(Runtime.getRuntime().availableProcessors());
        Batch batch = new Batch(profile, output);

        try (Connection connection = getConnection();
             PreparedStatement statement = getAllHeldURIsStatement(profile, connection);
             ResultSet resultSet = statement.executeQuery())
        {
            while (resultSet.next())
            {
                String bibMainEntityUri = resultSet.getString(1);

                if (bibMainEntityUri == null) // Necessary due to broken data :(
                    continue;

                if (exportedUris.contains(bibMainEntityUri))
                    continue;
                exportedUris.add(bibMainEntityUri);
                batch.bibUrisToConvert.add(bibMainEntityUri);

                if (batch.bibUrisToConvert.size() >= BATCH_SIZE)
                {
                    threadPool.submit(batch, this::executeBatch);
                    batch = new Batch(profile, output);
                }
            }
            if (!batch.bibUrisToConvert.isEmpty()) {
                threadPool.submit(batch, this::executeBatch);
            }
        }

        threadPool.awaitAllAndShutdown();
    }

    private void dumpSpecific(ExportProfile profile, Path idFilePath, MarcRecordWriter output)
            throws IOException, InterruptedException
    {
        SimplePool threadPool = BlockingThreadPool.simplePool(Runtime.getRuntime().availableProcessors());

        Batch batch = new Batch(profile, output);

        List<String> ids = Files.readAllLines(idFilePath);

        for (String id : ids)
        {
            if (exportedUris.contains(id))
                continue;
            exportedUris.add(id);
            batch.bibUrisToConvert.add(id);

            if (batch.bibUrisToConvert.size() >= BATCH_SIZE)
            {
                threadPool.submit(batch, this::executeBatch);
                batch = new Batch(profile, output);
            }
        }
        if (!batch.bibUrisToConvert.isEmpty())
            threadPool.submit(batch, this::executeBatch);

        threadPool.awaitAllAndShutdown();
    }

    private void executeBatch(Batch batch)
    {
        try
        {
            for (String bibUri : batch.bibUrisToConvert)
            {
                String systemID = m_whelk.getStorage().getSystemIdByIri(bibUri);
                if (systemID == null) {
                    log.warn("BibURI " + bibUri + " not found in system, skipping...");
                    continue;
                }

                Document document = m_whelk.loadEmbellished(systemID);

                Vector<MarcRecord> result = MarcExport.compileVirtualMarcRecord(batch.profile, document, m_whelk, m_toMarcXmlConverter);
                if (result == null) // A conversion error will already have been logged.
                    continue;


                for (MarcRecord mr : result)
                {
                    try
                    {
                        synchronized (this)
                        {
                            batch.output.writeRecord(mr);
                        }
                    } catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                }

            }
        } catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private Connection getConnection()
            throws SQLException
    {
        Connection connection = m_whelk.getStorage().getOuterConnection();
        connection.setAutoCommit(false);
        return connection;
    }

    private PreparedStatement getAllHeldURIsStatement(ExportProfile profile, Connection connection)
            throws SQLException
    {
        String locations = profile.getProperty("locations", "");

        if (isObviouslyBadSql(locations)) // Not watertight, but this is not user-input. It's admin-input.
            throw new RuntimeException("SQL INJECTION SUSPECTED.");

        List<String> libraryUriList = Arrays.asList(locations.split(" "));
        String stringList = "'https://libris.kb.se/library/" + String.join("', 'https://libris.kb.se/library/", libraryUriList) + "'";

        String sql = "SELECT data#>>'{@graph,1,itemOf,@id}' FROM lddb WHERE collection = 'hold' AND data#>>'{@graph,1,heldBy,@id}' IN (£) AND deleted = false";
        sql = sql.replace("£", stringList);

        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        preparedStatement.setFetchSize(100);

        return preparedStatement;
    }

    private PreparedStatement getAllSaoAndAgentsStatement(Connection connection, Set<String> agentTypes)
            throws SQLException
    {

        String sql = "SELECT id FROM lddb WHERE " +
                "(data#>>'{@graph,1,inScheme,@id}' = 'https://id.kb.se/term/sao' OR data#>>'{@graph,1,@type}' = ANY(?) ) " +
                "AND deleted = false";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setArray(1, connection.createArrayOf("TEXT", agentTypes.toArray()));
        preparedStatement.setFetchSize(100);

        return preparedStatement;
    }

    private PreparedStatement getAllAuthStatement(Connection connection)
            throws SQLException
    {
        String sql = "SELECT id FROM lddb WHERE collection = 'auth' AND deleted = false";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setFetchSize(100);

        return preparedStatement;
    }

    private boolean isObviouslyBadSql(String sql)
    {
        String[] badWords =
                {
                        "DROP",
                        "TRUNCATE",
                        "MODIFY",
                        "ALTER",
                        "UPDATE",
                };

        for (String word : badWords)
            if (StringUtils.containsIgnoreCase(sql, word))
                return true;
        return false;
    }
}
