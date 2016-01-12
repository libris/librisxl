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
import java.time.ZonedDateTime;
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

    public static void streamResponse(ResultSet resultSet, HttpServletResponse res)
            throws IOException, XMLStreamException, SQLException
    {
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(res.getOutputStream());

        writeOaiPmhHeader(writer);

        while (resultSet.next())
        {
            String data = resultSet.getString("data");
            String manifest = resultSet.getString("manifest");
            HashMap<String, Object> datamap = new ObjectMapper().readValue(data, HashMap.class);
            HashMap<String, Object> manifestmap = new ObjectMapper().readValue(manifest, HashMap.class);
            Document jsonLDdoc = new Document(datamap, manifestmap);


            System.out.println("DB item: " + jsonLDdoc.getId());
            //res.getOutputStream().write(jsonLDdoc.getId().getBytes());
            //res.getOutputStream().write("\n".getBytes());
                /*JsonLD2MarcXMLConverter converter = new JsonLD2MarcXMLConverter();
                Document marcXMLDoc = converter.convert(jsonLDdoc);
                System.out.println(marcXMLDoc.getData());
                PrintWriter out = res.getWriter();
                out.print(datat);
                out.flush();*/
        }

        writeOaiPmhClose(writer);
    }

    public static void sendOaiPmhError(String errorCode, String extraMessage, HttpServletResponse res)
            throws IOException, XMLStreamException
    {
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(res.getOutputStream());

        writeOaiPmhHeader(writer);

        writer.writeStartElement("error");
        writer.writeAttribute("code", errorCode);
        writer.writeCharacters(extraMessage);
        writer.writeEndElement();

        writeOaiPmhClose(writer);
    }

    public static ZonedDateTime parseISO8601(String dateTimeString)
    {
        if (dateTimeString == null)
            return null;
        if (dateTimeString.length() == 10) // Date only
            dateTimeString += "T00:00:00Z";
        return ZonedDateTime.parse(dateTimeString);
    }

    private static void writeOaiPmhHeader(XMLStreamWriter writer)
            throws IOException, XMLStreamException
    {
        writer.writeStartDocument("UTF-8", "1.0");
        writer.writeStartElement("OAI-PMH");
        writer.writeDefaultNamespace("http://www.openarchives.org/OAI/2.0/");
        writer.writeNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
        writer.writeAttribute("http://www.w3.org/2001/XMLSchema-instance", "schemaLocation", "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd");
    }

    private static void writeOaiPmhClose(XMLStreamWriter writer)
            throws IOException, XMLStreamException
    {
        writer.writeEndElement();
        writer.writeEndDocument();
        writer.close();
    }

    private void sendResponse(HttpServletRequest req, HttpServletResponse res) throws IOException
    {
        String verb = req.getParameter("verb");
        if (verb == null)
            res.sendError(400, "Correct OAI-PMH verb required.");

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
                    res.sendError(400, "Correct OAI-PMH verb required.");
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
