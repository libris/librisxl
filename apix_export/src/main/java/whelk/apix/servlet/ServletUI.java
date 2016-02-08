package whelk.apix.servlet;

import whelk.util.PropertyLoader;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Properties;

public class ServletUI extends HttpServlet implements UI
{
    final static int PSEUDO_CONSOLE_LINES = 64;

    ExporterThread m_exporterThread = null;
    String[] m_pseudoConsole = new String[PSEUDO_CONSOLE_LINES];
    int m_pseudoConsoleNext = 0;

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException
    {
        res.getOutputStream().print( getPseudoConsole() );
    }

    public void init()
    {
        Properties apixProperties = PropertyLoader.loadProperties("apix");

        m_exporterThread = new ExporterThread(apixProperties, null, null, this);
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
        m_pseudoConsole[m_pseudoConsoleNext++] = text;
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
