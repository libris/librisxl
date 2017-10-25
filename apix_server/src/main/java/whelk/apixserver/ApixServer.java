package whelk.apixserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Document;
import whelk.Whelk;
import whelk.component.PostgreSQLComponent;
import whelk.util.PropertyLoader;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Properties;

/**
 * A reimplementation of the APIX protocol, quirks and all.
 *
 * Notes on old APIX ID to new APIX ID mapping:
 * APIX relies on passing IDs as path segments, like so: .../bib/123/newhold
 * libris (XL) IDs cannot conform to this as they are themselves URIs,
 * which would then result in access URIs like: .../bib/https://libris.kb.se/uh23kjbsdkfbsk/newhold.
 *
 * (Is /newhold a part of the ID-URI, or the end of the enclosing URI?)
 *
 * This (very odd) form of ID would also break backwards compatibility as existing
 * clients will surely be splitting on this or that many "/" to get at the numerical
 * part of the ID. and "https:" makes for a bad identifier.
 *
 * Therefore APIX (as an EXCEPTION) is allowed to expose the internal XL-ID
 * (15/16 char hash) as if it was an old numerical bibid.
 *
 * In other words:
 * .../apix/0.1/cat/libris/bib/123              [maps to/from]      http://libris.kb.se/bib/123
 * .../apix/0.1/cat/libris/bib/4juh23kjbsdkfbsk [maps to/from]      https://libris.kb.se/4juh23kjbsdkfbsk
 */
public class ApixServer extends HttpServlet
{
    static Whelk s_whelk;
    private static final Logger s_logger = LogManager.getLogger(ApixServer.class);

    final static int ERROR_PARAM_COUNT = 0xff01;
    final static int ERROR_EXTRA_PARAM = 0xff02;
    final static int ERROR_BAD_COLLECTION = 0xff03;
    final static int ERROR_DB_NOT_LIBRIS = 0xff04;
    final static int ERROR_CONVERSION_FAILED = 0xff05;

    public void init()
    {
        Properties configuration = PropertyLoader.loadProperties("secret");
        PostgreSQLComponent postgreSqlComponent =
                new PostgreSQLComponent(configuration.getProperty("sqlUrl"), configuration.getProperty("sqlMaintable"));
        s_whelk = new Whelk(postgreSqlComponent);
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException
    {
        try { doGet2(request, response); } catch (Exception e)
        {
            s_logger.error("Failed to process GET request.", e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    public void doPut(HttpServletRequest request, HttpServletResponse response) throws IOException
    {
        try { doPut2(request, response); } catch (Exception e)
        {
            s_logger.error("Failed to process PUT request.", e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    public void doDelete(HttpServletRequest request, HttpServletResponse response) throws IOException
    {
        try { doDelete2(request, response); } catch (Exception e)
        {
            s_logger.error("Failed to process DELETE request.", e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    public void doGet2(HttpServletRequest request, HttpServletResponse response) throws Exception
    {
        String[] parameters = Utils.getPathSegmentParameters(request);
        int expectedParameterCount = 3; // expect: /libris/bib/123
        if (!Utils.validateParameters(response, parameters, expectedParameterCount))
            return; // error response already sent

        String collection = parameters[1];
        String bibId = parameters[2];

        String xlUri = Utils.mapApixIDtoXlUri(bibId, collection);
        String xlShortId = s_whelk.getStorage().getSystemIdByIri(xlUri);
        if (xlShortId == null)
        {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        Document document = s_whelk.getStorage().load(xlShortId);
        String marcXmlString = Utils.convertToMarcXml(response, document);
        if (marcXmlString == null)
            return; // error response already sent

        Utils.send200Response(response, Xml.formatApixGetRecordResponse(marcXmlString, document, collection));
    }

    public void doDelete2(HttpServletRequest request, HttpServletResponse response) throws IOException
    {

    }

    public void doPut2(HttpServletRequest request, HttpServletResponse response) throws IOException
    {

    }
}