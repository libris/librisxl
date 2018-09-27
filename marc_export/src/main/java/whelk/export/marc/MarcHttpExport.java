package whelk.export.marc;

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

    public void init()
    {
        whelk = Whelk.createLoadedCoreWhelk();
        profileExport = new ProfileExport(whelk);
    }
    public void destroy() { }

    public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException
    {
        // Parameters
        HashMap<String, String> parameterMap = new HashMap<>();
        String queryString = req.getQueryString();
        String[] parameters = queryString.split("&");
        for (String parameter : parameters)
        {
            String[] parts = parameter.split("=");
            parameterMap.put(parts[0], parts[1]);
        }

        if (parameterMap.get("from") == null || ! isValidZonedDateTime(parameterMap.get("from")) ||
                parameterMap.get("until") == null || ! isValidZonedDateTime(parameterMap.get("until")))
        {
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
                    res.sendError(HttpServletResponse.SC_BAD_REQUEST, "Valid options for \"deleted\" are \"ignore\", \"export\" or \"email\"");
                    return;
            }
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
            profileExport.exportInto(output, profile, parameterMap.get("from"), parameterMap.get("until"), deleteMode);
        }
        catch (SQLException se)
        {
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
