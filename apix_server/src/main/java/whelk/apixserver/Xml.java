package whelk.apixserver;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * The madness, just to print some xml.
 */
public class Xml
{
    private static DocumentBuilderFactory docBuildfactory = DocumentBuilderFactory.newInstance();
    private static DocumentBuilder builder;
    private static TransformerFactory transformerFactory = TransformerFactory.newInstance();
    static
    {
        try
        {
            builder = docBuildfactory.newDocumentBuilder();
        } catch (ParserConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static String docToString(Document xmlDoc) throws TransformerException
    {
        DOMSource source = new DOMSource(xmlDoc);
        StringWriter writer = new StringWriter();
        StreamResult result = new StreamResult(writer);
        Transformer transformer = transformerFactory.newTransformer();
        transformer.transform(source, result);
        return writer.toString();
    }

    public static String formatApixErrorResponse(String message, int code) throws TransformerException
    {
        Document xmlDoc = builder.newDocument();
        Element apix = xmlDoc.createElement("apix");
        xmlDoc.appendChild(apix);
        apix.setAttribute("version", "0.1");
        apix.setAttribute("status", "ERROR");
        apix.setAttribute("error_code", ""+code);
        apix.setAttribute("error_message", message);

        return docToString(xmlDoc);
    }

    public static String formatApixGetRecordResponse(String marcXmlString, whelk.Document whelkDocument, String collection)
            throws TransformerException, IOException, SAXException
    {
        Document xmlDoc = builder.newDocument();

        Element apix = xmlDoc.createElement("apix");
        Element marcRecord = builder.parse(new InputSource(new StringReader(marcXmlString))).getDocumentElement();
        xmlDoc.appendChild(apix);
        apix.setAttribute("version", "0.1");
        apix.setAttribute("status", "OK");
        apix.setAttribute("operation", "GETRECORD");

        Element record = xmlDoc.createElement("record");
        apix.appendChild(record);

        Element identifier = xmlDoc.createElement("identifier");
        record.appendChild(identifier);
        identifier.setTextContent(whelkDocument.getThingIdentifiers().get(0));

        Element url = xmlDoc.createElement("url");
        record.appendChild(url);
        url.setTextContent("http://api.libris.kb.se/apix/0.1/cat/libris/" + collection + "/" + whelkDocument.getShortId());

        Element metadata = xmlDoc.createElement("metadata");
        record.appendChild(metadata);
        metadata.appendChild(xmlDoc.importNode(marcRecord, true));

        Element timestamp = xmlDoc.createElement("timestamp");
        record.appendChild(timestamp);
        timestamp.setTextContent(whelkDocument.getModified());

        // if bib - if request x-holdings : extra and holdings

        return docToString(xmlDoc);
    }
}
