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

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Properties;

/**
 * Call like so:
 * curl -v -XPOST "http://localhost:8080/marc_export/?from=2018-09-10T00:00:00Z&until=2018-12-01T00:00:00Z" --data-binary @./etc/export.properties
 */
public class MarcHttpExport extends HttpServlet
{
    private Whelk whelk = null;
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

    public void init()
    {
        whelk = Whelk.createLoadedCoreWhelk();
        profileExport = new ProfileExport(whelk);
    }
    public void destroy() { }

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

        ProfileExport.DELETE_MODE deleteMode = ProfileExport.DELETE_MODE.IGNORE; // default
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
                case "email":
                    deleteMode = ProfileExport.DELETE_MODE.SEND_EMAIL;
                    break;
                default:
                    failedRequests.labels("Invalid option for 'deleted'",
                            Integer.toString(HttpServletResponse.SC_BAD_REQUEST)).inc();
                    res.sendError(HttpServletResponse.SC_BAD_REQUEST, "Valid options for \"deleted\" are \"ignore\", \"export\" or \"email\"");
                    return;
            }
        }

        // Virtual deletion means: If there exists no holding for any of the locations in the profile for a bib record, consider that bib record deleted.
        boolean doVirtualDeletions = false;
        if (parameterMap.get("virtualDelete") != null && parameterMap.get("virtualDelete").equalsIgnoreCase("true"))
            doVirtualDeletions = true;

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

        String encoding = profile.getProperty("characterencoding");
        if (encoding.equals("Latin1Strip")) {
            encoding = "ISO-8859-1";
        }

        MarcRecordWriter output = null;
        if (profile.getProperty("format", "ISO2709").equalsIgnoreCase("MARCXML"))
            output = new MarcXmlRecordWriter(res.getOutputStream(), encoding);
        else
            output = new Iso2709MarcRecordWriter(res.getOutputStream(), encoding);

        try
        {
            profileExport.exportInto(output, profile, parameterMap.get("from"), parameterMap.get("until"), deleteMode, doVirtualDeletions);
        }
        catch (SQLException se)
        {
            logger.error(se);
            failedRequests.labels("Export failed",
                    Integer.toString(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)).inc();
            res.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
        finally
        {
            output.close();
        }
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
}
