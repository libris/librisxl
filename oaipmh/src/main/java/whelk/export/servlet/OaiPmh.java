package whelk.export.servlet;

import whelk.util.PropertyLoader;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class OaiPmh extends HttpServlet
{
    // OAI-PMH Error/Condition codes
    public final static String OAIPMH_ERROR_BAD_VERB = "badVerb";
    public final static String OAIPMH_ERROR_BAD_RESUMPTION_TOKEN = "badResumptionToken";
    public final static String OAIPMH_ERROR_BAD_ARGUMENT = "badArgument";
    public final static String OAIPMH_ERROR_CANNOT_DISSEMINATE_FORMAT = "cannotDisseminateFormat";
    public final static String OAIPMH_ERROR_ID_DOES_NOT_EXIST = "idDoesNotExist";
    public final static String OAIPMH_ERROR_NO_RECORDS_MATCH = "noRecordsMatch";
    public final static String OAIPMH_ERROR_NO_METADATA_FORMATS = "noMetadataFormats";
    public final static String OAIPMH_ERROR_NO_SET_HIERARCHY = "noSetHierarchy";

    public static Properties configuration;

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException
    {
        handleRequest(req, res);
    }

    public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException
    {
        handleRequest(req, res);
    }

    public void init()
    {
        configuration = PropertyLoader.loadProperties("secret");
        DataBase.init();
    }

    public void destroy()
    {
        DataBase.destroy();
    }

    private void handleRequest(HttpServletRequest req, HttpServletResponse res) throws IOException
    {
        String verb = req.getParameter("verb");
        if (verb == null)
            verb = "";

        try
        {
            switch (verb) {
                case "GetRecord":
                    break;
                case "Identify":
                    break;
                case "ListIdentifiers":
                    break;
                case "ListMetadataFormats":
                    break;
                case "ListRecords":
                    ListRecords.handleListRecordsRequest(req, res);
                    break;
                case "ListSets":
                    break;
                default:
                    ResponseCommon.sendOaiPmhError(OAIPMH_ERROR_BAD_VERB, "OAI-PMH verb must be one of [GetRecord, Identify, " +
                            "ListIdentifiers, ListMetadataFormats, ListRecords, ListSets].", req, res);
            }
        }
        catch (IOException | XMLStreamException e)
        {
            // These exceptions are to be expected in every case where a client/harvester closes or loses connection
            // while a response is being sent.
            // TODO: LOG BROKEN CLIENT PIPE!
        }
        catch (SQLException e)
        {
            e.printStackTrace();
            res.sendError(500);
            // TODO: LOG!
        }
    }
}
