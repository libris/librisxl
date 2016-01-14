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
    public final static Set<String> supportedFormats;
    final static String FORMAT_DUBLINCORE = "oai_dc";
    final static String FORMAT_MARCXML = "marcxml";
    final static String FORMAT_JSONLD = "jsonld";
    static
    {
        supportedFormats = new HashSet<String>();
        supportedFormats.add(FORMAT_DUBLINCORE);
        supportedFormats.add(FORMAT_MARCXML);
        supportedFormats.add(FORMAT_JSONLD);
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

    /**
     * Send an OAI-PMH error (and return true) if there are any more parameters than the expected ones in the request
     */
    public static boolean errorOnExtraParameters(HttpServletRequest request, HttpServletResponse response, String... expectedParameters)
            throws IOException, XMLStreamException
    {
        String unknownParameters = Helpers.getUnknownParameters(request, expectedParameters);
        if (unknownParameters != null)
        {
            sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "Request contained unknown parameter(s): " + unknownParameters, request, response);
            return true;
        }
        return false;
    }

    public static void writeOaiPmhHeader(XMLStreamWriter writer, HttpServletRequest request, boolean includeParameters)
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

    public static void writeOaiPmhClose(XMLStreamWriter writer)
            throws IOException, XMLStreamException
    {
        writer.writeEndElement();
        writer.writeEndDocument();
        writer.close();
    }

    public static void writeConvertedDocument(XMLStreamWriter writer, String format, Document jsonLDdoc)
            throws IOException, XMLStreamException
    {
        switch (format)
        {
            case FORMAT_JSONLD: {
                writer.writeCData(jsonLDdoc.getDataAsString());
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
