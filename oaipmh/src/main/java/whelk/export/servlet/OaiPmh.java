package whelk.export.servlet;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.util.PropertyLoader;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.sql.*;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

public class OaiPmh extends HttpServlet {

    public static Properties configuration;

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException
    {
        sendResponse(req, res);
    }

    public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException
    {
        sendResponse(req, res);
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

    private void sendResponse(HttpServletRequest req, HttpServletResponse res) throws IOException
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
                    ResponseCommon.sendOaiPmhError("badVerb", "OAI-PMH verb must be one of [GetRecord, Identify, " +
                            "ListIdentifiers, ListMetadataFormats, ListRecords, ListSets].", req, res);
            }
        }
        catch (XMLStreamException e)
        {
            e.printStackTrace();
            res.sendError(500);
            // TODO: LOG!
        }
        catch (SQLException e)
        {
            e.printStackTrace();
            res.sendError(500);
            // TODO: LOG!
        }
    }
}
