package whelk.export.marc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Whelk;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
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

        try
        {
            profileExport.exportInto(res.getOutputStream(), profile, whelk, parameterMap.get("from"), parameterMap.get("until"));
        } catch (SQLException se)
        {
            res.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
}
