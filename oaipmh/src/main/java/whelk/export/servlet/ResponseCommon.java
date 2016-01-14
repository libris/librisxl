package whelk.export.servlet;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.converter.JsonLD2DublinCoreConverter;
import whelk.converter.marc.JsonLD2MarcXMLConverter;

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
import java.util.HashSet;
import java.util.Set;

public class ResponseCommon
{
    final static String FORMAT_DUBLINCORE = "oai_dc";
    final static String FORMAT_MARCXML = "marcxml";
    final static String FORMAT_JSONLD = "jsonld";
    final static Set<String> supportedFormats;
    static
    {
        supportedFormats = new HashSet<String>();
        supportedFormats.add(FORMAT_DUBLINCORE);
        supportedFormats.add(FORMAT_MARCXML);
        supportedFormats.add(FORMAT_JSONLD);
    }

    /**
     * Stream the supplied resultSet back to the requesting harvester in proper OAI-PMH format.
     * Checking that the request is correctly formed in every way is assumed to have been done _before_
     * this method is called.
     *
     * @param setTypeName The name of the type of list, for example "records" for the ListRecords request.
     * @param itemTypeName The name of the type of items in the list, for example "record" for the ListRecords request.
     */
    public static void streamListResponse(ResultSet resultSet, HttpServletRequest request, HttpServletResponse response,
                                          String setTypeName, String itemTypeName)
            throws IOException, XMLStreamException, SQLException
    {
        // An inelegant (but the recommended) way of checking if the ResultSet is empty.
        // Avoids the need for "backing-up" which would prevent streaming of the ResultSet from the db.
        if (!resultSet.isBeforeFirst())
        {
            sendOaiPmhError(OaiPmh.OAIPMH_ERROR_NO_RECORDS_MATCH, "", request, response);
            return;
        }

        // Was the data ordered in a format we know?
        String requestedFormat = request.getParameter("metadataPrefix");
        if (!supportedFormats.contains(requestedFormat))
        {
            sendOaiPmhError(OaiPmh.OAIPMH_ERROR_CANNOT_DISSEMINATE_FORMAT, "Unsupported format: " + requestedFormat,
                    request, response);
            return;
        }

        response.setContentType("text/xml");
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        xmlOutputFactory.setProperty("escapeCharacters", false); // Inline xml must be left untouched.
        XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(response.getOutputStream());

        writeOaiPmhHeader(writer, request, true);
        writer.writeStartElement(setTypeName);

        while (resultSet.next())
        {
            String data = resultSet.getString("data");
            String manifest = resultSet.getString("manifest");

            writer.writeStartElement(itemTypeName);
            writeConvertedDocument(writer, requestedFormat, data, manifest);
            writer.writeEndElement(); // itemTypeName
        }

        writer.writeEndElement(); // setTypeName
        writeOaiPmhClose(writer);
    }

    /**
     * Send a properly formatted OAI-PMH error response to the requesting harvester.
     */
    public static void sendOaiPmhError(String errorCode, String extraMessage, HttpServletRequest request, HttpServletResponse response)
            throws IOException, XMLStreamException
    {
        response.setContentType("text/xml");
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(response.getOutputStream());

        // The OAI-PMH specification requires that parameters be echoed in response, unless the response has an error
        // code of badVerb or badArgument, in which case the parameters must be omitted.
        boolean includeParameters = true;
        if (errorCode.equals(OaiPmh.OAIPMH_ERROR_BAD_VERB) || errorCode.equals(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT))
            includeParameters = false;

        writeOaiPmhHeader(writer, request, includeParameters);

        writer.writeStartElement("error");
        writer.writeAttribute("code", errorCode);
        writer.writeCharacters(extraMessage);
        writer.writeEndElement();

        writeOaiPmhClose(writer);
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
            Enumeration parameterNames = request.getParameterNames();
            while (parameterNames.hasMoreElements())
            {
                String parameterName = (String) parameterNames.nextElement();
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

    private static void writeConvertedDocument(XMLStreamWriter writer, String format, String data, String manifest)
            throws IOException, XMLStreamException
    {
        HashMap datamap = new ObjectMapper().readValue(data, HashMap.class);
        HashMap manifestmap = new ObjectMapper().readValue(manifest, HashMap.class);
        Document jsonLDdoc = new Document(datamap, manifestmap);

        switch (format)
        {
            case FORMAT_JSONLD: {
                writer.writeCData(data);
                break;
            }
            case FORMAT_DUBLINCORE: {
                JsonLD2DublinCoreConverter converter = new JsonLD2DublinCoreConverter();
                String converted = (String) converter.convert(jsonLDdoc).getData().get("content");
                writer.writeCharacters(converted);
                break;
            }
            case FORMAT_MARCXML: {
                JsonLD2MarcXMLConverter converter = new JsonLD2MarcXMLConverter();
                String converted = (String) converter.convert(jsonLDdoc).getData().get("content");
                writer.writeCharacters(converted);
                break;
            }
            default:
                // TODO: LOG! Getting here means errors in code. Not handling supported format.
        }
    }
}
