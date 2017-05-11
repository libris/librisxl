package whelk.apix;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.component.ElasticSearch;
import whelk.component.PostgreSQLComponent;

import java.io.IOException;
import java.sql.*;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
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
    private final Whelk m_whelk;
    private final ObjectMapper m_mapper = new ObjectMapper();
    private final Logger s_logger = LoggerFactory.getLogger(this.getClass());
    private final Converter m_converter;

    private enum ApixOp
    {
        APIX_NEW,
        APIX_UPDATE,
        APIX_DELETE
    }

    private enum BatchSelection
    {
        BATCH_NEXT_TIMESTAMP, // Get all new/updated documents at the next timestamp (after m_exportNewerThan)
        BATCH_PREVIOUSLY_FAILED // Get all documents we've tried and failed to export
    }

    public ExporterThread(Properties properties, ZonedDateTime exportNewerThan, UI ui)
    {
        this.m_properties = properties;
        this.m_exportNewerThan = exportNewerThan;
        this.m_ui = ui;
        PostgreSQLComponent postgres = new PostgreSQLComponent(properties.getProperty("sqlUrl"), properties.getProperty("sqlMaintable"));
        ElasticSearch elastic = new ElasticSearch(properties.getProperty("elasticHost"), properties.getProperty("elasticCluster"), properties.getProperty("elasticIndex"));
        m_whelk = new Whelk(postgres, elastic);
        m_whelk.loadCoreData();
        m_converter = new Converter(m_whelk);
    }

    public void run()
    {
        String from = "[beginning of time]";
        if (m_exportNewerThan != null)
            from = m_exportNewerThan.format(DateTimeFormatter.ISO_ZONED_DATE_TIME);

        m_ui.outputText("Beginning export batch from: " + from + ".");
        s_logger.info("Beginning export batch from: " + from + ".");

        do
        {
            exportBatch(BatchSelection.BATCH_NEXT_TIMESTAMP);
            exportBatch(BatchSelection.BATCH_PREVIOUSLY_FAILED);
            try
            {
                sleep(2000);
            } catch (InterruptedException e) { /* ignore */ }
        }
        while (!stopAtOpportunity.get());

        m_ui.outputText("Export batch ended. Will do nothing more without user input.");
        s_logger.info("Export batch ended. Will do nothing more without user input.");
    }

    /**
     * A batch consists of all documents with identical (modified) timestamps, (or all previously failed documents).
     * Since timestamps are usually unique, this means that most batches have only a single document, sometimes however
     * there are batches of several documents. Especially after large import jobs.
     */
    private void exportBatch(BatchSelection batchSelection)
    {
        int successfullyExportedDocumentsCount = 0;
        int documentsInBatchCount = 0;

        try ( Connection connection = m_whelk.getStorage().getConnection();
              PreparedStatement statement = prepareStatement(connection, batchSelection);
              ResultSet resultSet = statement.executeQuery() )
        {
            ZonedDateTime modified = null;
            while( resultSet.next() )
            {
                modified = ZonedDateTime.ofInstant(resultSet.getTimestamp("modified").toInstant(), ZoneOffset.UTC);
                String id = resultSet.getString("id");
                String data = resultSet.getString("data");
                boolean deleted = resultSet.getBoolean("deleted");
                String collection = resultSet.getString("collection");
                String changedBy = resultSet.getString("changedBy");
                String changedIn = resultSet.getString("changedIn");
                HashMap datamap = m_mapper.readValue(data, HashMap.class);
                Document document = new Document(datamap);
                document.setId(id);
                String voyagerId = getVoyagerId(document);

                ApixOp operation;
                if (deleted)
                    operation = ApixOp.APIX_DELETE;
                else if (voyagerId != null)
                    operation = ApixOp.APIX_UPDATE;
                else
                    operation = ApixOp.APIX_NEW;

                try
                {
                    String assignedVoyagerId = exportDocument(document, operation, collection);

                    // If we managed to exported a previous failure (=removal of the failure flag),
                    // or the APIX operation was NEW (=we set a Voyager ID on the post)
                    // we need to do a minor update in lddb.
                    if (batchSelection == BatchSelection.BATCH_PREVIOUSLY_FAILED || assignedVoyagerId != null)
                    {
                        commitAtomicDocumentUpdate(id, assignedVoyagerId, false, changedIn, changedBy, collection, deleted);
                    }

                    ++successfullyExportedDocumentsCount;
                } catch (Throwable e)
                {
                    commitAtomicDocumentUpdate(id, null, true, changedIn, changedBy, collection, deleted);
                    m_ui.outputText("Failed to export " + id + ", will automatically try again at a later time.");
                    s_logger.error("Failed to export " + id + ", will automatically try again at a later time.", e);
                }
                ++documentsInBatchCount;
            }

			if (documentsInBatchCount > 0)
			{
			   switch (batchSelection)
			   {
				  case BATCH_NEXT_TIMESTAMP:
				  {
					 m_exportNewerThan = modified;
					 m_ui.setCurrentTimeStamp(m_exportNewerThan);
					 m_ui.outputText("Completed export of " + successfullyExportedDocumentsCount + " out of " + documentsInBatchCount + " document(s) with modified = " + modified);
					 break;
				  }
				  case BATCH_PREVIOUSLY_FAILED:
				  {
					 m_ui.outputText("Completed export of " + successfullyExportedDocumentsCount + " out of " + documentsInBatchCount + " document(s) queued for retry.");
					 break;
				  }
			   }
			}
			else
            {
                // If there were no new/changed documents to export, set the next time to search from to 3 hours ago,
                // if m_exportNewerThan is not already at a higher value.
                // This is to alleviate stress on the DB when there have been no changes in a long time, as we now never
                // need to look further back than 3 hours in history.
                // The point of the specific value of 3 hours is to be doubly safe with regards to daylight saving changes etc.
                ZonedDateTime searchFromNext = ZonedDateTime.now().minus(3, ChronoUnit.HOURS);
                if (searchFromNext.compareTo(m_exportNewerThan) > 0)
                {
                    m_exportNewerThan = searchFromNext;
                }
            }
        }
        catch (Throwable e)
        {
            StringBuilder callStack = new StringBuilder("");
            for (StackTraceElement frame : e.getStackTrace())
                callStack.append(frame.toString() + "\n");
            m_ui.outputText("Export batch stopped with exception: " + e + " Callstack:\n" + callStack);
            s_logger.error("Export batch stopped with exception. ", e);
        }
    }

    /**
     * Returns the assigned voyager id if ApixOp was NEW, otherwise returns null or throws
     */
    private String exportDocument(Document document, ApixOp operation, String collection)
            throws SQLException, IOException
    {
        String voyagerId = getVoyagerId(document);
        String voyagerDatabase = m_properties.getProperty("apixDatabase");

        switch (operation)
        {
            case APIX_DELETE:
            {
                String apixDocumentUrl = m_properties.getProperty("apixHost") + "/apix/0.1/cat/" + voyagerDatabase + voyagerId;
                apixRequest(apixDocumentUrl, "DELETE", null);
                return null;
            }
            case APIX_UPDATE:
            {
                String apixDocumentUrl = m_properties.getProperty("apixHost") + "/apix/0.1/cat/" + voyagerDatabase + voyagerId;
                String convertedText = m_converter.makeEmbellishedMarcJSONString(document, collection);
                apixRequest(apixDocumentUrl, "PUT", convertedText);
                return null;
            }
            case APIX_NEW:
            {
                String apixDocumentUrl = m_properties.getProperty("apixHost") + "/apix/0.1/cat/" + voyagerDatabase + "/" + collection + "/new";

                // Special treatment for new holdings.
                // Instead of /apix/0.1/cat/libris/hold/new it is: /apix/0.1/cat/libris/bib/1234/newhold,
                // so change apixDocumentUrl here:
                if (collection.equals("hold"))
                {
                    String bibId = document.getHoldingFor();
                    int charIndex = bibId.indexOf("/bib/");
                    String shortBibId = bibId.substring(charIndex + 5);
                    apixDocumentUrl = m_properties.getProperty("apixHost") + "/apix/0.1/cat/" + voyagerDatabase + "/bib/" + shortBibId + "/newhold";
                }

                String convertedText = m_converter.makeEmbellishedMarcJSONString(document, collection);
                String controlNumber = apixRequest(apixDocumentUrl, "PUT", convertedText);
                return controlNumber;
            }
        }
		return null;
    }

    private PreparedStatement prepareStatement(Connection connection, BatchSelection batchSelection)
            throws SQLException
    {
        switch (batchSelection)
        {
            case BATCH_NEXT_TIMESTAMP:
            {
                // Select the 'next' timestamp after m_exportNewerThan, and then select all documents with that timestamp (as 'modified'),
                // exclude everything in collection 'definitions' and everything with 'changedIn' = 'vcopy'.
                String sql = "SELECT id, data, modified, collection, changedIn, changedBy, deleted FROM " + m_properties.getProperty("sqlMaintable") +
                        " WHERE collection <> 'definitions' AND (changedIn <> 'vcopy' or changedIn is null) AND modified = " +
                        "(SELECT MIN(modified) FROM " + m_properties.getProperty("sqlMaintable") + " WHERE modified > ? AND collection <> 'definitions'" +
                        " AND (changedIn <> 'vcopy' or changedIn is null))";

                PreparedStatement statement = connection.prepareStatement(sql);

                // Postgres uses microsecond precision, we need match or exceed this to not risk SQL timestamps slipping between
                // truncated java timestamps
                Timestamp timestamp = new Timestamp(m_exportNewerThan.toInstant().toEpochMilli());
                timestamp.setNanos(m_exportNewerThan.toInstant().getNano());
                statement.setTimestamp(1, timestamp);

                return statement;
            }
            case BATCH_PREVIOUSLY_FAILED:
            {
                // Select any documents that _have_ a apix export failed flag
                String sql = "SELECT id, data, modified, collection, changedIn, changedBy, deleted FROM " + m_properties.getProperty("sqlMaintable") +
                        " WHERE data->'@graph' @> '[{\"" + JsonLd.getAPIX_FAILURE_KEY() + "\":true}]'";

                PreparedStatement statement = connection.prepareStatement(sql);
                return statement;
            }
        }

        return null; // unreachable
    }

    /**
     * Find the /auth/1234 style id in a document. Return null if none is found.
     */
    private String getVoyagerId(Document document)
    {
        final String idPrefix = "http://libris.kb.se/"; // Leave this hardcoded, as these URIs references voyager IDs, which do not change.

        List<String> ids = document.getRecordIdentifiers();
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
        s_logger.debug("APIX exporter sending http request: " + httpVerb + " to : " + url);
        LongTermHttpRequest request = new LongTermHttpRequest(url, httpVerb, "application/xml",
                data, m_properties.getProperty("apixUsername"), m_properties.getProperty("apixPassword"));
        int responseCode = request.getResponseCode();
        String responseData = request.getResponseData();

        switch (responseCode)
        {
            case 200:
            {
                // error in disguise? 200 is only legitimately returned on GET or DELETE. POST/PUT only returns 200 on error.
                if (httpVerb.equals("DELETE"))
                {
                    s_logger.info("APIX DELETE OK on: " + url);
                    return null;
                }

                if (responseData == null)
                    responseData = "";
                throw new IOException("APIX error: " + responseData);
            }
            case 201: // fine, happens on new
            case 303: // fine, happens on save/update
            {
                String location = request.getResponseHeaders().get("Location");
                s_logger.info("APIX PUT OK on: " + url + ", location returned: " + location);
                return parseControlNumberFromAPIXLocation(location);
            }
            default:
            {
                if (responseData == null)
                    responseData = "";
                throw new IOException("APIX responded on " + url + " with http " + responseCode + ": " + responseData);
            }
        }
    }

    private String parseControlNumberFromAPIXLocation(String urlLocation)
            throws IOException
    {
        String voyagerDatabase = m_properties.getProperty("apixDatabase");
        Pattern pattern = Pattern.compile("0.1/cat/" + voyagerDatabase + "/(auth|bib|hold)/(\\d+)\\z");
        Matcher matcher = pattern.matcher(urlLocation);
        if (matcher.find())
        {
            return matcher.group(2);
        }
        throw new IOException("Could not parse control number from APIX location header: " + urlLocation);
    }

    /**
     * Update a document (with control number or failed flag) in a safe atomic manner (without risking overwrite of
     * anyone else's changes)
     * Will also reindex the document in elastic.
     * Will not update the documents "modified" column.
     */
    private void commitAtomicDocumentUpdate(String id, String newVoyagerId, boolean failedExport, String changedIn,
                                            String changedBy, String collection, boolean deleted)
            throws IOException, SQLException
    {
        m_whelk.storeAtomicUpdate(id, true, changedIn, changedBy, collection, deleted,
                (Document doc) ->
                {
                    if (newVoyagerId != null)
                    {
                        doc.addRecordIdentifier("http://libris.kb.se/" + collection + "/" + newVoyagerId);
                        doc.addThingIdentifier("http://libris.kb.se/resource/" + collection + "/" + newVoyagerId);
                        doc.setControlNumber(newVoyagerId);
                    }
                    doc.setApixExportFailFlag(failedExport);
                });
    }

    private PreparedStatement prepareSelectStatement(Connection connection, String id)
            throws SQLException
    {
        String sql = "SELECT data FROM " + m_properties.getProperty("sqlMaintable") +
                " WHERE id = ?";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, id);
        return statement;
    }
}
