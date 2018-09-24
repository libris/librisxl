package whelk.export.marc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Whelk;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

public class MarcHttpExport extends HttpServlet
{
    public Whelk whelk;
    private final Logger logger = LogManager.getLogger(this.getClass());

    public MarcHttpExport()
    {
        whelk = Whelk.createLoadedCoreWhelk();
    }

    public void init() { }
    public void destroy() { }

    public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException
    {
        Properties props = new Properties();
        props.load(new StringReader("")); // profileString
        se.kb.libris.export.ExportProfile profile = new se.kb.libris.export.ExportProfile(props);

        res.getOutputStream().write("OK.\n".getBytes());
    }
}
