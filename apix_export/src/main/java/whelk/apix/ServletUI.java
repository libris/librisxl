package whelk.apix;

import whelk.util.PropertyLoader;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
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
            case "/endpointdb":
                res.getOutputStream().print( m_properties.getProperty("apixDatabase") );
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

    public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException, ServletException
    {
        String path = req.getPathInfo();
        if (path == null)
        {
            res.sendError(404);
            return;
        }

        switch (path)
        {
            case "/start":
            {
                BufferedReader reader = new BufferedReader(new InputStreamReader(req.getInputStream()));
                if (m_exporterThread == null || m_exporterThread.getState() == Thread.State.TERMINATED)
                {
                    ZonedDateTime from = parseDateTime(reader.readLine());
                    m_exporterThread = new ExporterThread(m_properties, from, this);
                    m_exporterThread.start();
                }
                else
                    outputText("Already running, ignoring start command");
                break;
            }
            case "/stop":
            {
                if (m_exporterThread != null)
                {
                    m_exporterThread.stopAtOpportunity.set(true);
                    try {
                        m_exporterThread.join();
                    } catch (InterruptedException e) {}
                }
                break;
            }
            default:
                res.sendError(404);
        }
    }

    public void init()
    {
        m_properties = PropertyLoader.loadProperties("secret");
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
        ZonedDateTime now = ZonedDateTime.now();
        m_pseudoConsole[m_pseudoConsoleNext++] = now.format(DateTimeFormatter.ISO_ZONED_DATE_TIME) + ":&nbsp;&nbsp;" + text;
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

            //output.append(m_pseudoConsole[next]);
            //output.append("\n");
            output.insert(0, m_pseudoConsole[next] + "\n");
        }

        return  output.toString();
    }

    private ZonedDateTime parseDateTime(String stringTime)
    {
        if (stringTime == null || stringTime.equals("null"))
            return null;

        try
        {
            return ZonedDateTime.parse(stringTime, DateTimeFormatter.ISO_ZONED_DATE_TIME);
        } catch (DateTimeParseException e)
        {
            return null;
        }
    }
}
