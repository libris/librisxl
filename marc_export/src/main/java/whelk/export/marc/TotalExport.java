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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

public class TotalExport
{
    private final int BATCH_SIZE = 200;
    private JsonLD2MarcXMLConverter m_toMarcXmlConverter;
    private Whelk m_whelk;

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
        if (args.length != 4)
            printUsageAndExit();

        ExportProfile profile = new ExportProfile(new File(args[2]));
        long size = Long.parseLong(args[3]);
        long segment = Long.parseLong(args[4]);

        String encoding = profile.getProperty("characterencoding");
        if (encoding.equals("Latin1Strip")) {
            encoding = "ISO-8859-1";
        }
        MarcRecordWriter output = null;
        if (profile.getProperty("format", "ISO2709").equalsIgnoreCase("MARCXML"))
            output = new MarcXmlRecordWriter(System.out, encoding);
        else
            output = new Iso2709MarcRecordWriter(System.out, encoding);

        new TotalExport(Whelk.createLoadedCoreWhelk()).dump(profile, size, segment, output);
    }

    private static void printUsageAndExit()
    {
        System.out.println("Usage: java -Dxl.secret.properties=SECRETPROPSFILE -jar marc_export.jar PROFILE-FILE SEGMENT-SIZE SEGMENT");
        System.out.println("");
        System.out.println("   PROFILE-FILE should be a Java-properties file with the export-profile settings.");
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

    private void executeBatch(Batch batch, int threadIndex)
    {
        Connection connection = m_whelk.getStorage().getConnection();
        for (String bibUri : batch.bibUrisToConvert)
        {
            String systemID = m_whelk.getStorage().getSystemIdByIri(bibUri, connection);
            Document document = m_whelk.getStorage().loadEmbellished(systemID, m_whelk.getJsonld());

            Vector<MarcRecord> result = MarcExport.compileVirtualMarcRecord(batch.profile, document, m_whelk, m_toMarcXmlConverter);
            if (result == null) // A conversion error will already have been logged.
                continue;


            for (MarcRecord mr : result)
            {
                try
                {
                    batch.output.writeRecord(mr);
                } catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }

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
        String stringList = "'" + String.join("', '", libraryUriList) + "'";

        String sql = "SELECT data#>>'{@graph,1,itemOf,@id}' FROM lddb WHERE collection = 'hold' AND data#>>'{@graph,1,heldBy,@id}' IN (£)";
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
