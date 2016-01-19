package whelk.export.servlet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whelk.Document;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Enumeration;

public class ResponseCommon
{
    private static final Logger logger = LoggerFactory.getLogger(ResponseCommon.class);

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

        writeOaiPmhClose(writer, request);
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

    public static void writeOaiPmhClose(XMLStreamWriter writer, HttpServletRequest req)
            throws IOException, XMLStreamException
    {
        writer.writeEndElement();
        writer.writeEndDocument();
        writer.close();
        logger.info("Response sent successfully to {}:{}.", req.getRemoteAddr(), req.getRemotePort());
    }

    public static void writeConvertedDocument(XMLStreamWriter writer, String formatPrefix, Document jsonLDdoc)
            throws IOException, XMLStreamException
    {
        OaiPmh.FormatDescription formatDescription = OaiPmh.supportedFormats.get(formatPrefix);

        // Convert if the format has a converter (otherwise assume jsonld)
        String convertedText = null;
        if (formatDescription.converter != null)
        {
            Document convertedDoucment = formatDescription.converter.convert(jsonLDdoc);
            convertedText = (String) convertedDoucment.getData().get("content");
        }
        else
            convertedText = jsonLDdoc.getDataAsString();

        // If the format is not XML, it needs to be embedded as CDATA, to not interfere with the response XML format.
        if (formatDescription.isXmlFormat)
            writer.writeCharacters(convertedText);
        else
            writer.writeCData(convertedText);
    }
}
