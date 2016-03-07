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
    // This atomic boolean may be toggled from outside, causing the thread to stop exporting and return
    public AtomicBoolean stopAtOpportunity = new AtomicBoolean(false);

    // The "from" parameter. The exporter will export everything with a (modified) timestamp > this value.
    private ZonedDateTime m_exportNewerThan;

    private final Properties m_properties;
    private final UI m_ui;
    private final PostgreSQLComponent m_postgreSQLComponent;
    private final ObjectMapper m_mapper = new ObjectMapper();
    private final JsonLD2MarcXMLConverter m_converter = new JsonLD2MarcXMLConverter();

    private enum ApixOp
    {
        APIX_NEW,
        APIX_UPDATE,
        APIX_DELETE
    }

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
            try
            {
                sleep(2000);
            } catch (InterruptedException e) { /* ignore */ }
        }
        while (!stopAtOpportunity.get());

        m_ui.outputText("Export batch ended. Will do nothing more without user input.");
    }

    /**
     * A batch consists of all documents with identical (modified) timestamps. Since timestamps are usually unique, this
     * means that most batches have only a single document, sometimes however there are batches of several documents.
     * Especially after large import jobs.
     */
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
        String data = resultSet.getString("data");
        String manifest = resultSet.getString("manifest");
        boolean deleted = resultSet.getBoolean("deleted");

        HashMap datamap = m_mapper.readValue(data, HashMap.class);
        HashMap manifestmap = m_mapper.readValue(manifest, HashMap.class);
        Document document = new Document(datamap, manifestmap);

        String collection = document.getCollection();
        String voyagerId = getVoyagerId(document);
        String voyagerDatabase = m_properties.getProperty("apixDatabase");

        ApixOp operation;
        if (deleted)
            operation = ApixOp.APIX_DELETE;
        else if (voyagerId != null)
            operation = ApixOp.APIX_UPDATE;
        else
            operation = ApixOp.APIX_NEW;

        switch (operation)
        {
            case APIX_DELETE:
            {
                String apixDocumentUrl = m_properties.getProperty("apixHost") + "/apix/0.1/cat/" + voyagerDatabase + voyagerId;
                apixRequest(apixDocumentUrl, "DELETE", null);
                break;
            }
            case APIX_UPDATE:
            {
                String apixDocumentUrl = m_properties.getProperty("apixHost") + "/apix/0.1/cat/" + voyagerDatabase + voyagerId;
                Document convertedDoucment = m_converter.convert(document);
                String convertedText = (String) convertedDoucment.getData().get("content");
                apixRequest(apixDocumentUrl, "PUT", convertedText);
                break;
            }
            case APIX_NEW:
            {
                String apixDocumentUrl = m_properties.getProperty("apixHost") + "/apix/0.1/cat/" + voyagerDatabase + "/" + collection + "/new";
                Document convertedDoucment = m_converter.convert(document);
                String convertedText = (String) convertedDoucment.getData().get("content");
                String controlNumber = apixRequest(apixDocumentUrl, "PUT", convertedText);
                document.setControlNumber(controlNumber);
                break;
            }
        }
    }

    private PreparedStatement prepareStatement(Connection connection)
            throws SQLException
    {
        // Select the 'next' timestamp after m_exportNewerThan, and then select all documents with that timestamp (as 'modified')
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

    /**
     * Find the /auth/1234 style id in a document. Return null if none is found.
     */
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
     * Returns the assigned voyager control number on successful PUT. null on successful DELETE. Otherwise throws an error.
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
                if (httpVerb.equals("DELETE"))
                    return null;
                
                if (responseData == null)
                    responseData = "";
                throw new IOException("APIX error: " + responseData);
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
    }

    private String parseControlNumberFromAPIXLocation(String urlLocation)
            throws IOException
    {
        String voyagerDatabase = m_properties.getProperty("apixDatabase");
        Pattern pattern = Pattern.compile("0.1/cat/" + voyagerDatabase + "/(auth|bib|hold)/(\\d+)");
        Matcher matcher = pattern.matcher(urlLocation);
        if (matcher.find())
        {
            return matcher.group(2);
        }
        throw new IOException("Could not parse control number from APIX location header.");
    }
}
