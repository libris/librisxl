package whelk.export.marc;

import org.apache.commons.lang3.StringUtils;
import se.kb.libris.export.ExportProfile;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.Iso2709MarcRecordWriter;
import se.kb.libris.util.marc.io.MarcRecordWriter;
import se.kb.libris.util.marc.io.MarcXmlRecordWriter;
import whelk.Document;
import whelk.Whelk;
import whelk.converter.marc.JsonLD2MarcXMLConverter;
import whelk.util.MarcExport;
import whelk.util.ThreadPool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

public class TotalExport
{
    private final int BATCH_SIZE = 200;
    private JsonLD2MarcXMLConverter m_toMarcXmlConverter;
    private Whelk m_whelk;
    private Set<String> exportedUris = new TreeSet<>();

    final static Logger log = LogManager.getLogger(TotalExport.class);

    public TotalExport(Whelk whelk)
    {
        m_whelk = whelk;
        m_toMarcXmlConverter = new JsonLD2MarcXMLConverter(whelk.createMarcFrameConverter());
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
            throws IOException, SQLException, InterruptedException
    {
        if (args.length != 2 && args.length != 3)
            printUsageAndExit();

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
            new TotalExport(Whelk.createLoadedCoreWhelk()).dumpSpecific(profile, idFilePath, output);
        } else if (args.length == 3)
        {
            long size = Long.parseLong(args[1]);
            long segment = Long.parseLong(args[2]);

            new TotalExport(Whelk.createLoadedCoreWhelk()).dump(profile, size, segment, output);
        }
        output.close();
    }

    private static void printUsageAndExit()
    {
        System.out.println("Usage:");
        System.out.println("");
        System.out.println("  java -Dxl.secret.properties=SECRETPROPSFILE -jar marc_export.jar PROFILE-FILE ID-FILE");
        System.out.println("or");
        System.out.println("  java -Dxl.secret.properties=SECRETPROPSFILE -jar marc_export.jar PROFILE-FILE SEGMENT-SIZE SEGMENT");
        System.out.println("");
        System.out.println("   PROFILE-FILE should be a Java-properties file with the export-profile settings.");
        System.out.println("   ID-FILE should be a file with IDs to export, containing one URI per row.");
        System.out.println("   SEGMENT-SIZE is the number of records to dump in each segment.");
        System.out.println("   SEGMENT is the number of the segment to be dumped.");
        System.out.println("");
        System.out.println("For example:");
        System.out.println(" java -jar marc_export.jar export.properties 1000 1");
        System.out.println("Would generate the second segment (each consisting of 1000 records) of all records held by whatever");
        System.out.println("is in location=[] in export.properties.");
        System.out.println("");
        System.out.println("To not use segmentation, both SEGMENT and SEGMENT-SIZE should be '0'.");
        System.exit(1);
    }

    private void dump(ExportProfile profile, long size, long segment, MarcRecordWriter output)
            throws SQLException, InterruptedException
    {
        ThreadPool threadPool = new ThreadPool(4 * Runtime.getRuntime().availableProcessors());
        Batch batch = new Batch(profile, output);

        try (Connection connection = getConnection();
             PreparedStatement statement = getAllHeldURIsStatement(profile, size, size*segment, connection);
             ResultSet resultSet = statement.executeQuery())
        {
            while (resultSet.next())
            {
                String bibMainEntityUri = resultSet.getString(1);
                if (exportedUris.contains(bibMainEntityUri))
                    continue;
                exportedUris.add(bibMainEntityUri);
                batch.bibUrisToConvert.add(bibMainEntityUri);

                if (batch.bibUrisToConvert.size() >= BATCH_SIZE)
                {
                    threadPool.executeOnThread(batch, this::executeBatch);
                    batch = new Batch(profile, output);
                }
            }
            if (!batch.bibUrisToConvert.isEmpty())
                threadPool.executeOnThread(batch, this::executeBatch);
        }

        threadPool.joinAll();
    }

    private void dumpSpecific(ExportProfile profile, Path idFilePath, MarcRecordWriter output)
            throws IOException, InterruptedException
    {
        ThreadPool threadPool = new ThreadPool(4 * Runtime.getRuntime().availableProcessors());
        Batch batch = new Batch(profile, output);

        List<String> ids = Files.readAllLines(idFilePath);

        for (String id : ids)
        {
            if (exportedUris.contains(id))
                return;
            exportedUris.add(id);
            batch.bibUrisToConvert.add(id);

            if (batch.bibUrisToConvert.size() >= BATCH_SIZE)
            {
                threadPool.executeOnThread(batch, this::executeBatch);
                batch = new Batch(profile, output);
            }
        }
        if (!batch.bibUrisToConvert.isEmpty())
            threadPool.executeOnThread(batch, this::executeBatch);

        threadPool.joinAll();
    }

    private void executeBatch(Batch batch, int threadIndex)
    {
        try (Connection connection = m_whelk.getStorage().getConnection())
        {
            for (String bibUri : batch.bibUrisToConvert)
            {
                String systemID = m_whelk.getStorage().getSystemIdByIri(bibUri, connection);
                if (systemID == null) {
                    log.warn("BibURI " + bibUri + " not found in system, skipping...");
                    continue;
                }

                Document document = m_whelk.getStorage().loadEmbellished(systemID, m_whelk.getJsonld());

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
        } catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    private Connection getConnection()
            throws SQLException
    {
        Connection connection = m_whelk.getStorage().getConnection();
        connection.setAutoCommit(false);
        return connection;
    }

    private PreparedStatement getAllHeldURIsStatement(ExportProfile profile, long limit, long offset, Connection connection)
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

        if (limit != 0)
        {
            sql += " ORDER BY created LIMIT ? OFFSET ?";
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setLong(1, limit);
            preparedStatement.setLong(2, offset);
        }
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
