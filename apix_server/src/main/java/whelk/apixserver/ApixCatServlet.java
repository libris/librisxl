package whelk.apixserver;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import se.kb.libris.util.marc.*;
import se.kb.libris.util.marc.io.MarcXmlRecordReader;
import se.kb.libris.util.marc.io.MarcXmlRecordWriter;
import whelk.Document;
import whelk.util.LegacyIntegrationTools;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.ListIterator;

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
public class ApixCatServlet extends HttpServlet
{
    private static final Logger s_logger = LogManager.getLogger(ApixCatServlet.class);

    final static int ERROR_PARAM_COUNT = 0xff01;
    final static int ERROR_EXTRA_PARAM = 0xff02;
    final static int ERROR_BAD_COLLECTION = 0xff03;
    final static int ERROR_DB_NOT_LIBRIS = 0xff04;
    final static int ERROR_CONVERSION_FAILED = 0xff05;

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

        Document document = Utils.getXlDocument(bibId, collection);
        if (document == null)
        {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        String marcXmlString = Utils.convertToMarcXml(document);
        if (marcXmlString == null)
        {
            s_logger.error("Conversion to MARC failed for " + document.getCompleteId());
            Utils.send200Response(response, Xml.formatApixErrorResponse("Conversion to MARC failed.", ApixCatServlet.ERROR_CONVERSION_FAILED));
            return;
        }

        List<Document> attachedHoldings = null;
        if (collection.equals("bib") && request.getParameter("x-holdings") != null && request.getParameter("x-holdings").equalsIgnoreCase("true"))
        {
            attachedHoldings = Utils.s_whelk.getStorage().getAttachedHoldings(document.getThingIdentifiers());
        }

        Utils.send200Response(response, Xml.formatApixGetRecordResponse(marcXmlString, document, collection, attachedHoldings));
    }

    public void doDelete2(HttpServletRequest request, HttpServletResponse response) throws Exception
    {
        String[] parameters = Utils.getPathSegmentParameters(request);
        int expectedParameterCount = 3; // expect: /libris/bib/123
        if (!Utils.validateParameters(response, parameters, expectedParameterCount))
            return; // error response already sent

        String collection = parameters[1];
        String bibId = parameters[2];

        String xlUri = Utils.mapApixIDtoXlUri(bibId, collection);
        String xlShortId = Utils.s_whelk.getStorage().getSystemIdByIri(xlUri);
        if (xlShortId == null)
        {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        Utils.s_whelk.remove(xlShortId, Utils.APIX_SYSTEM_CODE, request.getRemoteUser(), collection);
        s_logger.info("Successful delete on: " + xlShortId);
        Utils.send200Response(response, "");
    }

    public void doPut2(HttpServletRequest request, HttpServletResponse response) throws Exception
    {
        String[] parameters = Utils.getPathSegmentParameters(request);

        if (parameters.length == 2)
        {
            Digidaily.saveDigiDaily(request, response);
        }
        else if (parameters.length == 3)
        {
            saveOnId(request, response);
        } else if (parameters.length == 4)
        {
            saveHoldOfBib(request, response);
        }
    }

    private void saveOnId(HttpServletRequest request, HttpServletResponse response) throws Exception
    {
        String[] parameters = Utils.getPathSegmentParameters(request);

        int expectedParameterCount = 3; // expect: /libris/bib/{new/123}
        if (!Utils.validateParameters(response, parameters, expectedParameterCount))
            return; // error response already sent

        String collection = parameters[1];
        String id = parameters[2];

        String content = IOUtils.toString(request.getReader());
        Document incomingDocument = Utils.convertToRDF(content, collection, null);
        if (incomingDocument == null)
        {
            Utils.send200Response(response, Xml.formatApixErrorResponse("Conversion from MARC failed.", ApixCatServlet.ERROR_CONVERSION_FAILED));
            return;
        }

        if (id.equalsIgnoreCase("new"))
        {
            Utils.s_whelk.store(incomingDocument, Utils.APIX_SYSTEM_CODE, request.getRemoteUser(), collection, false);
            s_logger.info("Successful new on : " + incomingDocument.getShortId());
            Utils.send201Response(response, Utils.APIX_BASEURI + "/0.1/cat/libris/" + collection + "/" + incomingDocument.getShortId());
        } else // save/overwrite existing
        {
            incomingDocument.setId(id);
            Utils.s_whelk.storeAtomicUpdate(id, false, Utils.APIX_SYSTEM_CODE, request.getRemoteUser(), collection, false,
                    (Document doc) ->
                    {
                        doc.data = incomingDocument.data;
                    });
            s_logger.info("Successful update on : " + incomingDocument.getShortId());
            Utils.send303Response(response, Utils.APIX_BASEURI + "/0.1/cat/libris/" + collection + "/" + incomingDocument.getShortId());
        }
    }

    private void saveHoldOfBib (HttpServletRequest request, HttpServletResponse response) throws Exception
    {
        String[] parameters = Utils.getPathSegmentParameters(request);

        int expectedParameterCount = 4; // expect: /libris/bib/123/newhold
        if (!Utils.validateParameters(response, parameters, expectedParameterCount))
            return; // error response already sent

        String collection = parameters[1];
        String bibid = parameters[2];
        String operator = parameters[3];

        String content = IOUtils.toString(request.getReader());
        Document incomingDocument = Utils.convertToRDF(content, "hold", bibid);
        if (incomingDocument == null)
        {
            Utils.send200Response(response, Xml.formatApixErrorResponse("Conversion from MARC failed.", ApixCatServlet.ERROR_CONVERSION_FAILED));
            return;
        }

        if (!operator.equals("newhold"))
        {
            Utils.send200Response(response, Xml.formatApixErrorResponse("Unexpected parameter: " + operator, ApixCatServlet.ERROR_EXTRA_PARAM));
            return;
        }

        Utils.s_whelk.store(incomingDocument, Utils.APIX_SYSTEM_CODE, request.getRemoteUser(), collection, false);
        s_logger.info("Successful new (hold on bib) on : " + incomingDocument.getShortId());
        Utils.send201Response(response, Utils.APIX_BASEURI + "/0.1/cat/libris/" + collection + "/" + incomingDocument.getShortId());
    }
}