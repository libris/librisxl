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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExporterThread extends Thread
{
    private static final String VOYAGER_DATABASE = "test"; // "libris" !

    // This atomic boolean may be toggled from outside, causing the thread to stop exporting and return
    public AtomicBoolean stopAtOpportunity = new AtomicBoolean(false);

    // The "from" parameter. The exporter will export everything with a (modified) timestamp >= this value.
    // When running in continuous mode (no end date), this value will be updated with every batch (to the latest)
    // timestamp in that batch.
    private ZonedDateTime m_exportNewerThan;

    private final Properties m_properties;
    private final UI m_ui;
    private final PostgreSQLComponent m_postgreSQLComponent;
    private final ObjectMapper mapper = new ObjectMapper();

    public ExporterThread(Properties properties, ZonedDateTime exportNewerThan, UI ui)
    {
        this.m_properties = properties;
        this.m_exportNewerThan = exportNewerThan;
        this.m_ui = ui;
        this.m_postgreSQLComponent = new PostgreSQLComponent(properties.getProperty("sqlUrl"), properties.getProperty("sqlMaintable"));
    }

    public void run()
    {
        String from = "[beginning of time]";
        if (m_exportNewerThan != null)
            from = m_exportNewerThan.format(DateTimeFormatter.ISO_ZONED_DATE_TIME);

        m_ui.outputText("Beginning export batch from: " + from + ".");

        do
        {
            exportBatch();
            //m_ui.outputText("Exported " + exportedDocumentsCount + " documents.");
            try
            {
                sleep(2000);
            } catch (InterruptedException e) { /* ignore */ }
        }
        while (!stopAtOpportunity.get());

        m_ui.outputText("Export batch ended. Will do nothing more without user input.");
    }

    private void exportBatch()
    {
        int exportedDocumentsCount = 0;

        try ( Connection connection = m_postgreSQLComponent.getConnection();
              PreparedStatement statement = prepareStatement(connection);
              ResultSet resultSet = statement.executeQuery() )
        {
            ZonedDateTime modified = null;
            while( resultSet.next() )
            {
                // Each resultset will hold all rows with a specific 'modified' time. Usually there will only be one,
                // row in the resultset but even if there are more than one, they are all guaranteed to have the same
                // 'modified' time.
                modified = ZonedDateTime.ofInstant(resultSet.getTimestamp("modified").toInstant(), ZoneOffset.UTC);

                try
                {
                    exportDocument(resultSet);
                } catch (Exception e)
                {
                    // TODO: Call for human intervention? Log? Cannot silently ignore a failure to replicate data to Voyager
                }
                ++exportedDocumentsCount;
            }

            // We now know that all rows with this specific 'modified' timestamp have been exported ok. The next batch
            // will use the next following 'modified' timestamp
            if (exportedDocumentsCount > 0)
            {
                m_exportNewerThan = modified;
                m_ui.outputText("Completed export of " + exportedDocumentsCount + " document(s) with modified = " + m_exportNewerThan);
            }
        }
        catch (Exception e)
        {
            StringBuilder callStack = new StringBuilder("");
            for (StackTraceElement frame : e.getStackTrace())
                callStack.append(frame.toString() + "\n");
            m_ui.outputText("Export batch stopped with exception: " + e + " Callstack:\n" + callStack);
        }
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

        String collection = document.getCollection();

        String apixDocumentUrl = m_properties.getProperty("apixHost") + "/apix/0.1/cat/" + VOYAGER_DATABASE + "/new";
        String voyagerId = getVoyagerId(document);
        if (voyagerId != null)
            apixDocumentUrl = m_properties.getProperty("apixHost") + "/apix/0.1/cat/" + VOYAGER_DATABASE + voyagerId;

        if (deleted && voyagerId != null)
        {
            apixRequest(apixDocumentUrl, "DELETE", null);
        }
        else
        {
            JsonLD2MarcXMLConverter converter = new JsonLD2MarcXMLConverter();
            Document convertedDoucment = converter.convert(document);
            String convertedText = (String) convertedDoucment.getData().get("content");
            apixRequest(apixDocumentUrl, "PUT", convertedText);
        }
    }

    private PreparedStatement prepareStatement(Connection connection)
            throws SQLException
    {
        String sql = "SELECT id, manifest, data, modified, deleted FROM " + m_properties.getProperty("sqlMaintable") +
                " WHERE manifest->>'changedIn' <> 'vcopy' AND modified = (SELECT MIN(modified) FROM lddb WHERE modified > ?)";

        PreparedStatement statement = connection.prepareStatement(sql);

        // Postgres uses microsecond precision, we need match or exceed this to not risk SQL timetamps slipping between
        // truncated java timestamps
        Timestamp timestamp = new Timestamp(m_exportNewerThan.toInstant().toEpochMilli());
        timestamp.setNanos(m_exportNewerThan.toInstant().getNano());
        statement.setTimestamp(1, timestamp);

        return statement;
    }

    private String getVoyagerId(Document document)
    {
        final String idPrefix = Document.getBASE_URI().toString(); // https://libris.kb.se/

        List<String> ids = document.getIdentifiers();
        for (String id: ids)
        {
            if (id.startsWith(idPrefix))
            {
                String potentialId = id.substring(idPrefix.length()-1);
                if (potentialId.startsWith("/auth/") ||
                        potentialId.startsWith("/bib/") ||
                        potentialId.startsWith("/hold/"))
                return potentialId;
            }
        }

        return null;
    }

    /**
     * Returns the assigned voyager control number, or null on failure (or throws).
     */
    private String apixRequest(String url, String httpVerb, String data)
            throws IOException
    {
        LongTermHttpRequest request = new LongTermHttpRequest(url, httpVerb, "application/xml", data);
        int responseCode = request.getResponseCode();
        String responseData = request.getResponseData();

        switch (responseCode)
        {
            case 200:
            {
                // error in disguise? 200 is only legitimately returned on GET or DELETE. POST/PUT only returns 200 on error.
                if (!httpVerb.equals("GET") && !httpVerb.equals("DELETE"))
                    throw new IOException("APIX error: " + responseData);
                break;
            }
            case 201: // fine, happens on new
            case 303: // fine, happens on save/update
            {
                String location = request.getResponseHeaders().get("Location");
                return parseControlNumberFromAPIXLocation(location);
            }
            default:
            {
                if (responseData == null)
                    responseData = "";
                throw new IOException("APIX responded with http " + responseCode + ": " + responseData);
            }
        }
        return null;
    }

    private String parseControlNumberFromAPIXLocation(String urlLocation)
    {
        Pattern pattern = Pattern.compile("0.1/cat/" + VOYAGER_DATABASE + "/(auth|bib|hold)/(\\d+)");
        Matcher matcher = pattern.matcher(urlLocation);
        if (matcher.find())
        {
            return matcher.group(2);
        }
        return null;
    }
}
