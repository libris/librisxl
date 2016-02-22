package whelk.apix;

import whelk.util.PropertyLoader;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Properties;

public class ServletUI extends HttpServlet implements UI
{
    final static int PSEUDO_CONSOLE_LINES = 64;

    ExporterThread m_exporterThread = null;
    String[] m_pseudoConsole = new String[PSEUDO_CONSOLE_LINES];
    int m_pseudoConsoleNext = 0;
    Properties m_properties = null;

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException, ServletException
    {
        String path = req.getPathInfo();
        if (path == null)
        {
            res.sendError(404);
            return;
        }

        res.setContentType("text/plain");

        switch (path)
        {
            case "/console":
                res.getOutputStream().print( getPseudoConsole() );
                break;
            case "/endpoint":
                res.getOutputStream().print( m_properties.getProperty("apixHost") );
                break;
            case "/startpoint":
                String jdbcUrl = m_properties.getProperty("sqlUrl");
                if (jdbcUrl.contains("@"))
                    jdbcUrl = jdbcUrl.substring( jdbcUrl.indexOf("@")+1 );
                res.getOutputStream().print( jdbcUrl );
                break;
            default:
                res.sendError(404);
        }
    }

    public void init()
    {
        m_properties = PropertyLoader.loadProperties("secret");

        m_exporterThread = new ExporterThread(m_properties, null, null, this);
        m_exporterThread.start();
    }

    public void destroy()
    {
        m_exporterThread.stopAtOpportunity.set(true);
        try {
            m_exporterThread.join();
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public void outputText(String text)
    {
        ZonedDateTime utc = ZonedDateTime.now(ZoneOffset.UTC);
        m_pseudoConsole[m_pseudoConsoleNext++] = utc.toString() + ":&nbsp;&nbsp;" + text;
        if (m_pseudoConsoleNext > PSEUDO_CONSOLE_LINES-1)
            m_pseudoConsoleNext = 0;
    }

    /**
     * Construct a string representation of the pseudo console output (the last N lines of generated output)
     */
    private String getPseudoConsole()
    {
        int next = m_pseudoConsoleNext;
        StringBuilder output = new StringBuilder("");

        for (int i = 0; i < PSEUDO_CONSOLE_LINES; ++i)
        {
            --next;
            if (next < 0)
                next = PSEUDO_CONSOLE_LINES - 1;

            if (m_pseudoConsole[next] == null)
                break;

            output.append(m_pseudoConsole[next]);
            output.append("\n");
        }

        return  output.toString();
    }
}
