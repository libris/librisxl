package whelk.apixserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Document;
import whelk.JsonLd;
import whelk.converter.marc.JsonLD2MarcXMLConverter;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.transform.TransformerException;
import java.io.IOException;

public class Utils
{
    private static JsonLD2MarcXMLConverter s_toMarcConverter = new JsonLD2MarcXMLConverter();
    private static final Logger s_logger = LogManager.getLogger(Utils.class);

    static String convertToMarcXml(Document document) throws TransformerException, IOException
    {
        try
        {
            return (String) s_toMarcConverter.convert(document.data, document.getShortId()).get(JsonLd.getNON_JSON_CONTENT_KEY());
        }
        catch (Exception | Error e)
        {
            return null;
        }
    }

    static String mapApixIDtoXlUri(String apixID, String collection)
    {
        // strictly numerical positive id, less than 15 chars means an old voyager ID (bibid)
        if (apixID.matches("\\d+") && apixID.length() < 15)
        {
            String voyagerIdUri = "http://libris.kb.se/" + collection + "/" + apixID;
            return ApixServer.s_whelk.getStorage().getRecordId(voyagerIdUri);
        }
        else
            return Document.getBASE_URI().toString() + apixID;
    }

    static String[] getPathSegmentParameters(HttpServletRequest request)
    {
        String pathInfo = request.getPathInfo().trim();
        if (pathInfo.startsWith("/"))
            pathInfo = pathInfo.substring(1);
        if (pathInfo.endsWith("/"))
            pathInfo = pathInfo.substring(0, pathInfo.length()-1);
        return pathInfo.split("/");
    }

    static boolean validateParameters(HttpServletResponse response, String[] pathSegments, int expectedParameterCount)
            throws TransformerException, IOException
    {
        if (pathSegments.length != expectedParameterCount)
        {
            send200Response(response, Xml.formatApixErrorResponse("Expected " + expectedParameterCount +
                    " segments after /cat/ but there was " + pathSegments.length, ApixServer.ERROR_PARAM_COUNT));
            return false;
        }
        if ( pathSegments.length > 0 && !pathSegments[0].equals("libris") )
        {
            send200Response(response, Xml.formatApixErrorResponse("Database path segment must be \"libris\"", ApixServer.ERROR_DB_NOT_LIBRIS));
            return false;
        }
        if ( pathSegments.length > 1 &&
                !pathSegments[1].equals("bib") && !pathSegments[1].equals("auth") && !pathSegments[1].equals("hold"))
        {
            send200Response(response,
                    Xml.formatApixErrorResponse("Collection segment must be \"bib\", \"auth\" or \"bib\"",  ApixServer.ERROR_BAD_COLLECTION));
            return false;
        }
        if ( pathSegments.length > 3 && !pathSegments[3].equals("newhold"))
        {
            send200Response(response,
                    Xml.formatApixErrorResponse("Unknown extra path segment: " + pathSegments[3],  ApixServer.ERROR_EXTRA_PARAM));
            return false;
        }
        return true;
    }

    static void send200Response(HttpServletResponse response, String message) throws IOException
    {
        response.setStatus(HttpServletResponse.SC_OK);
        ServletOutputStream out = response.getOutputStream();
        out.print(message);
        out.close();
    }
}
