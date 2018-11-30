package whelk.export.marc;

import org.apache.commons.lang3.StringUtils;
import se.kb.libris.export.ExportProfile;
import whelk.Whelk;
import whelk.converter.marc.JsonLD2MarcXMLConverter;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class TotalExport
{
    private JsonLD2MarcXMLConverter m_toMarcXmlConverter;
    private Whelk m_whelk;
    public TotalExport(Whelk whelk)
    {
        m_whelk = whelk;
        m_toMarcXmlConverter = new JsonLD2MarcXMLConverter(whelk.createMarcFrameConverter());
    }

    public static void main(String[] args)
            throws IOException, SQLException
    {
        // The idea is to choose holdings based on profile locations, and order them by created-date.
        // This will require a btree index on created (we would use IDs if they were sequential).
        // This will enable us to do total dumps, with predictable start/offsets, meaning total dumps
        // can be made as sequences of parts where necessary.

        // Necessary parameters: A profile, a 'number' of records to include (possibly infinite), and a
        // part number. The result will be all 'number' of records after/offset part*number.

        // Using export.properties, get record 1001 -> 2000
        // java -jar marc_export.jar export.properties 1000 1

        if (args.length != 4)
            printUsageAndExit();

        ExportProfile profile = new ExportProfile(new File(args[2]));
        long size = Long.parseLong(args[3]);
        long segment = Long.parseLong(args[4]);
    }

    private void dump(ExportProfile profile, long size, long segment)
            throws SQLException
    {
        try (Connection connection = getConnection();
             PreparedStatement statement = getAllHeldURIs(profile, size, size*segment, connection);
             ResultSet resultSet = statement.executeQuery())
        {
            while (resultSet.next())
            {
                String bibMainEntityUri = resultSet.getString(1);
                // BATCH THAT SHIT AND DUMP IT CONCURRENTLY
            }
        }
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
        System.exit(1);
    }

    private Connection getConnection()
            throws SQLException
    {
        Connection connection = m_whelk.getStorage().getConnection();
        connection.setAutoCommit(false);
        return connection;
    }

    private PreparedStatement getAllHeldURIs(ExportProfile profile, long limit, long offset, Connection connection)
            throws SQLException
    {
        String locations = profile.getProperty("locations", "");

        if (isObviouslyBadSql(locations)) // Not watertight, but this is not user-input, it's admin-input.
            throw new RuntimeException("SQL INJECTION SUSPECTED.");

        List<String> libraryUriList = Arrays.asList(locations.split(" "));
        String stringList = "'" + String.join("', '", libraryUriList) + "'";

        String sql = "SELECT data#>>'{@graph,1,itemOf,@id}' FROM lddb WHERE collection = 'hold' AND data#>>'{@graph,1,heldBy,@id}' IN (£) ORDER BY created LIMIT ? OFFSET ?";
        sql = sql.replace("£", stringList);

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, limit);
        preparedStatement.setLong(2, offset);
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
