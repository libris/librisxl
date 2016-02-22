package whelk.apix;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.component.PostgreSQLComponent;
import whelk.converter.marc.JsonLD2MarcXMLConverter;

import java.io.IOException;
import java.sql.*;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExporterThread extends Thread
{
    /**
     * This atomic boolean may be toggled from outside, causing the thread to stop exporting and return
     */
    public AtomicBoolean stopAtOpportunity = new AtomicBoolean(false);

    // The "from" parameter. The exporter will export everything with a (modified) timestamp >= this value.
    private final ZonedDateTime m_exportNewerThan;

    // The "until" parameter. The exporter will export everything with a (modified) timestamp < this value.
    private final ZonedDateTime m_exportOlderThan;

    private final Properties m_properties;
    private final UI m_ui;
    private final PostgreSQLComponent m_postgreSQLComponent;
    private final ObjectMapper mapper = new ObjectMapper();

    public ExporterThread(Properties properties, ZonedDateTime exportNewerThan,
                          ZonedDateTime exportOlderThan, UI ui)
    {
        this.m_properties = properties;
        this.m_exportNewerThan = exportNewerThan;
        this.m_exportOlderThan = exportOlderThan;
        this.m_ui = ui;
        this.m_postgreSQLComponent = new PostgreSQLComponent(properties.getProperty("sqlUrl"), properties.getProperty("sqlMaintable"));
    }

    public void run()
    {
        String from = "[beginning of time]";
        String until = "[end of time]";
        if (m_exportNewerThan != null)
            from = m_exportNewerThan.format(DateTimeFormatter.ISO_ZONED_DATE_TIME);
        if (m_exportOlderThan != null)
            until = m_exportOlderThan.format(DateTimeFormatter.ISO_ZONED_DATE_TIME);

        m_ui.outputText("Beginning export batch\n\tfrom: " + from + "\n\tuntil: " + until + ".");

        do
        {
            int exportedDocumentsCount = exportBatch();
            m_ui.outputText("Exported " + exportedDocumentsCount + " documents.");
            try
            {
                sleep(500);
            } catch (InterruptedException e) { /* ignore */ }
        }
        while (m_exportOlderThan != null);

        m_ui.outputText("Export batch ended. Will do nothing more without user input.");
    }

    private int exportBatch()
    {
        int exportedDocumentsCount = 0;

        try ( Connection connection = m_postgreSQLComponent.getConnection();
              PreparedStatement statement = prepareStatement(connection);
              ResultSet resultSet = statement.executeQuery() )
        {
            while( resultSet.next() )
            {
                exportDocument(resultSet);
                ++exportedDocumentsCount;

                if (stopAtOpportunity.get())
                {
                    String id = resultSet.getString("id");
                    m_ui.outputText("Export batch cancelled! Last exported document: " + id);
                    break;
                }
            }
        }
        catch (Exception e)
        {
            StringBuilder callStack = new StringBuilder("");
            for (StackTraceElement frame : e.getStackTrace())
                callStack.append(frame.toString() + "\n");
            m_ui.outputText("Export batch stopped with exception: " + e + " Callstack:\n " + callStack);
        }

        return exportedDocumentsCount;
    }

    private void exportDocument(ResultSet resultSet)
            throws SQLException, IOException
    {
        String id = resultSet.getString("id");
        String data = resultSet.getString("data");
        String manifest = resultSet.getString("manifest");
        ZonedDateTime modified = ZonedDateTime.ofInstant(resultSet.getTimestamp("modified").toInstant(), ZoneOffset.UTC);
        boolean deleted = resultSet.getBoolean("deleted");

        HashMap datamap = mapper.readValue(data, HashMap.class);
        HashMap manifestmap = mapper.readValue(manifest, HashMap.class);
        Document document = new Document(datamap, manifestmap);

        JsonLD2MarcXMLConverter converter = new JsonLD2MarcXMLConverter();
        Document convertedDoucment = converter.convert(document);
        String convertedText = (String) convertedDoucment.getData().get("content");
    }

    private PreparedStatement prepareStatement(Connection connection)
            throws SQLException
    {
        String sql = "SELECT id, manifest, data, modified, deleted FROM " + m_properties.getProperty("sqlMaintable") +
                " WHERE TRUE ";
        if (m_exportNewerThan != null)
            sql += "AND modified >= ? ";
        if (m_exportOlderThan != null)
            sql += "AND modified < ? ";

        int parameterIndex = 1;
        PreparedStatement statement = connection.prepareStatement(sql);
        if (m_exportNewerThan != null)
            statement.setTimestamp(parameterIndex++, new Timestamp(m_exportNewerThan.toInstant().getEpochSecond() * 1000L));
        if (m_exportOlderThan != null)
            statement.setTimestamp(parameterIndex++, new Timestamp(m_exportOlderThan.toInstant().getEpochSecond() * 1000L));
        statement.setFetchSize(64);

        return statement;
    }
}
