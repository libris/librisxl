package whelk.export.servlet;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.converter.JsonLD2DublinCoreConverter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Enumeration;
import java.util.HashMap;

public class ResponseCommon
{
    public static void streamResponse(ResultSet resultSet, HttpServletRequest request, HttpServletResponse response)
            throws IOException, XMLStreamException, SQLException
    {
        response.setContentType("text/xml");
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        xmlOutputFactory.setProperty("escapeCharacters", false); // Inline xml must be left untouched.
        XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(response.getOutputStream());

        writeOaiPmhHeader(writer, request, true);

        while (resultSet.next())
        {
            String data = resultSet.getString("data");
            String manifest = resultSet.getString("manifest");
            HashMap<String, Object> datamap = new ObjectMapper().readValue(data, HashMap.class);
            HashMap<String, Object> manifestmap = new ObjectMapper().readValue(manifest, HashMap.class);
            Document jsonLDdoc = new Document(datamap, manifestmap);

            JsonLD2DublinCoreConverter converter = new JsonLD2DublinCoreConverter();
            String converted = (String) converter.convert(jsonLDdoc).getData().get("content");
            writer.writeCharacters(converted);

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

    public static void sendOaiPmhError(String errorCode, String extraMessage, HttpServletRequest request, HttpServletResponse response)
            throws IOException, XMLStreamException
    {
        response.setContentType("text/xml");
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(response.getOutputStream());

        // The OAI-PMH specification requires that parameters be echoed in response, unless the response has an error
        // code of badVerb or badArgument, in which case the parameters must be omitted.
        boolean includeParameters = true;
        if (errorCode.equals("badVerb") || errorCode.equals("badArgument"))
            includeParameters = false;

        writeOaiPmhHeader(writer, request, includeParameters);

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

    private static void writeOaiPmhHeader(XMLStreamWriter writer, HttpServletRequest request, boolean includeParameters)
            throws IOException, XMLStreamException
    {
        // Static header
        writer.writeStartDocument("UTF-8", "1.0");
        writer.writeStartElement("OAI-PMH");
        writer.writeDefaultNamespace("http://www.openarchives.org/OAI/2.0/");
        writer.writeNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
        writer.writeAttribute("http://www.w3.org/2001/XMLSchema-instance", "schemaLocation",
                "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd");

        // Mandatory time element
        writer.writeStartElement("responseDate");
        writer.writeCharacters( ZonedDateTime.now(ZoneOffset.UTC).toString() );
        writer.writeEndElement();

        // Mandatory request element
        writer.writeStartElement("request");
        if (includeParameters)
        {
            Enumeration<String> parameterNames = request.getParameterNames();
            while (parameterNames.hasMoreElements())
            {
                String parameterName = parameterNames.nextElement();
                String parameterValue = request.getParameter(parameterName);
                writer.writeAttribute(parameterName, parameterValue);
            }
        }
        writer.writeCharacters( request.getRequestURL().toString() );
        writer.writeEndElement();
    }

    private static void writeOaiPmhClose(XMLStreamWriter writer)
            throws IOException, XMLStreamException
    {
        writer.writeEndElement();
        writer.writeEndDocument();
        writer.close();
    }
}
