package whelk.export.marc;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import se.kb.libris.util.marc.io.Iso2709MarcRecordWriter;
import se.kb.libris.util.marc.io.MarcRecordWriter;
import se.kb.libris.util.marc.io.MarcXmlRecordWriter;
import whelk.Whelk;
import whelk.exception.WhelkRuntimeException;
import whelk.util.http.CoreWhelkHttpServlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Call like so:
 * curl -v -XPOST "http://localhost:8080/marc_export/?from=2018-09-10T00:00:00Z&until=2018-12-01T00:00:00Z" --data-binary @./etc/export.properties
 */
public class MarcHttpExport extends CoreWhelkHttpServlet
{
    private ProfileExport profileExport = null;
    private final Logger logger = LogManager.getLogger(this.getClass());

    static final Counter requests = Counter.build()
        .name("marc_export_api_requests_total").help("Total requests to API.")
        .register();

    static final Counter failedRequests = Counter.build()
        .name("marc_export_api_failed_requests_total").help("Total failed requests to API.")
        .labelNames("reason", "status").register();

    static final Gauge ongoingRequests = Gauge.build()
        .name("marc_export_api_ongoing_requests_total").help("Total ongoing API requests.")
        .register();

    static final Summary requestsLatency = Summary.build()
        .name("marc_export_api_requests_latency_seconds")
        .help("API request latency in seconds.")
        .register();

    @Override
    protected void init(Whelk whelk)
    {
        profileExport = new ProfileExport(whelk, whelk.getStorage().createAdditionalConnectionPool("ProfileExport"));
    }

    public void destroy() {
        profileExport.shutdown();
    }

    public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException
    {
        requests.inc();
        ongoingRequests.inc();
        Summary.Timer requestTimer = requestsLatency.startTimer();

        try
        {
            doPost2(req, res);
        }
        finally
        {
            ongoingRequests.dec();
            requestTimer.observeDuration();
        }
    }

    private void doPost2(HttpServletRequest req, HttpServletResponse res) throws IOException
    {
        // Random unique ID to identify the request across log statements
        String requestId = UUID.randomUUID().toString();
        // Parameters
        HashMap<String, String> parameterMap = new HashMap<>();
        String queryString = req.getQueryString();
        if (queryString == null)
        {
            failedRequests.labels("Missing required params",
                    Integer.toString(HttpServletResponse.SC_BAD_REQUEST)).inc();
            res.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required parameters \"from\" and \"until\".");
            return;
        }

        String[] parameters = queryString.split("&");
        for (String parameter : parameters)
        {
            String[] parts = parameter.split("=");
            parameterMap.put(parts[0], parts[1]);
        }

        if (parameterMap.get("from") == null || ! isValidZonedDateTime(parameterMap.get("from")) ||
                parameterMap.get("until") == null || ! isValidZonedDateTime(parameterMap.get("until")))
        {
            failedRequests.labels("Missing required params",
                    Integer.toString(HttpServletResponse.SC_BAD_REQUEST)).inc();
            res.sendError(HttpServletResponse.SC_BAD_REQUEST, "Valid \"from\" and \"until\" parameters are required, for example like so: 2018-09-10T00:00:00Z");
            return;
        }

        // Body (export profile)
        StringBuilder body = new StringBuilder();
        BufferedReader reader = req.getReader();
        String line;
        do
        {
            line = reader.readLine();
            body.append( line );
            body.append( "\n" );
        } while (line != null);
        Properties props = new Properties();
        props.load(new StringReader(body.toString()));
        se.kb.libris.export.ExportProfile profile = new se.kb.libris.export.ExportProfile(props);

        logger.info("Handling export request {}, remote IP {} (x-forwarded-for: {}), query: {}, parsed parameters: {}, properties: {}",
                requestId, req.getRemoteAddr(), req.getHeader("x-forwarded-for"), queryString, parameterMap, props);

        ProfileExport.DELETE_MODE deleteMode = ProfileExport.DELETE_MODE.IGNORE; // Default
        if (profile.getProperty("exportdeleted", "OFF").equalsIgnoreCase("ON"))
            deleteMode = ProfileExport.DELETE_MODE.EXPORT;

        // Virtual deletion means: If there exists no holding for any of the locations in the profile for a bib record, consider that bib record deleted.
        boolean doVirtualDeletions = profile.getProperty("virtualdelete", "OFF").equalsIgnoreCase("ON"); // Default

        // For backwards compatibility, respect any "deleted" and "virtualDelete" HTTP parameters.
        // We now normally prefer these settings to live in the profile (exportdeleted/virtualdelete),
        // but HTTP parameters take precedence, where they exist.
        {
            if (parameterMap.get("deleted") != null)
            {
                switch (parameterMap.get("deleted"))
                {
                    case "ignore":
                        deleteMode = ProfileExport.DELETE_MODE.IGNORE;
                        break;
                    case "export":
                        deleteMode = ProfileExport.DELETE_MODE.EXPORT;
                        break;
                    case "append":
                        deleteMode = ProfileExport.DELETE_MODE.SEPARATE;
                        break;
                    default:
                        logger.error("Failed to handle request {} due to invalid option for 'deleted'", requestId);
                        failedRequests.labels("Invalid option for 'deleted'",
                                Integer.toString(HttpServletResponse.SC_BAD_REQUEST)).inc();
                        res.sendError(HttpServletResponse.SC_BAD_REQUEST, "Valid options for \"deleted\" are \"ignore\", \"export\" or \"append\"");
                        return;
                }
            }

            if (parameterMap.get("virtualDelete") != null) {
                if (parameterMap.get("virtualDelete").equalsIgnoreCase("true")) {
                    doVirtualDeletions = true;
                }
                if (parameterMap.get("virtualDelete").equalsIgnoreCase("false")) {
                    doVirtualDeletions = false;
                }
            }
        }

        String encoding = profile.getProperty("characterencoding");
        if (encoding == null)
            encoding = "UTF-8";
        if (encoding.equals("Latin1Strip")) {
            encoding = "ISO-8859-1";
        }

        MarcRecordWriter output = null;
        OutputStream outStream = res.getOutputStream();
        if (profile.getProperty("format", "ISO2709").equalsIgnoreCase("MARCXML"))
            output = new MarcXmlRecordWriter(outStream, encoding);
        else
            output = new Iso2709MarcRecordWriter(outStream, encoding);

        Map<String, ProfileExport.DELETE_REASON> deleteInfo = null;

        try
        {
            var p = new ProfileExport.Parameters(profile, parameterMap.get("from"), parameterMap.get("until"), deleteMode, doVirtualDeletions);
            deleteInfo = profileExport.exportInto(output, p);
        }
        catch (SQLException | WhelkRuntimeException e)
        {
            logger.error("Failed to handle export request {}: {}", requestId, e);
            failedRequests.labels("Export failed",
                    Integer.toString(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)).inc();
            res.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);

            if (isFatalError(e)) {
                logger.fatal("FATAL ERROR detected in export request {}, exiting to allow systemd restart", requestId, e);
                System.exit(1);
            }
        }
        finally
        {
            output.close();
        }

        if (deleteMode == ProfileExport.DELETE_MODE.SEPARATE)
        {
            outStream.write(0); // Use a \0 (null-byte) to delimit the export from the accompanying delete manifest CSV.
            outStream.write("Libris bibliografiska kontrollnummer rekommenderade för gallring;Anledning\n".getBytes(StandardCharsets.UTF_8));
            for (String id : deleteInfo.keySet())
            {
                String csvline = id;
                switch (deleteInfo.get(id))
                {
                    case DELETED:
                        csvline += ";Gallrad ur Libris.\n";
                        break;
                    case VIRTUALLY_DELETED:
                        csvline += ";Ni har ej längre bestånd på posten.\n";
                        break;
                }
                outStream.write(csvline.getBytes(StandardCharsets.UTF_8));
            }
        }
        outStream.close();
        logger.info("Finished handling export request {}", requestId);
    }

    private boolean isValidZonedDateTime(String candidate)
    {
        try
        {
            ZonedDateTime.parse(candidate);
        } catch (DateTimeParseException e)
        {
            return false;
        }
        return true;
    }

    private boolean isFatalError(Throwable e)
    {
        Throwable current = e;
        while (current != null)
        {
            // Maybe also consider IllegalStateException to be a fatal error?
            if (current instanceof OutOfMemoryError) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
