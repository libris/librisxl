package whelk.converter

import org.w3c.dom.Element
import whelk.Document

import javax.xml.parsers.DocumentBuilder
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.stream.XMLOutputFactory
import javax.xml.stream.XMLStreamWriter

/**
 * An extremely minimal and very lossy converter from jsonld to dublin core. Only identifiers are included.
 */
class JsonLD2DublinCoreConverter implements FormatConverter
{
    public Document convert(Document doc)
    {
        HashMap<String, String> data = new HashMap<String, String>();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(baos);
        writer.writeStartElement("oai_dc", "dc", "http://www.openarchives.org/OAI/2.0/oai_dc/");
        writer.writeNamespace("oai_dc", "http://www.openarchives.org/OAI/2.0/oai_dc/");
        writer.writeNamespace("dc", "http://purl.org/dc/elements/1.1/");
        writer.writeNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
        writer.writeAttribute("http://www.w3.org/2001/XMLSchema-instance", "schemaLocation",
                "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd");

        writer.writeStartElement("http://purl.org/dc/elements/1.1/", "identifier");
        writer.writeCharacters(doc.getId());
        writer.writeEndElement();

        writer.writeEndElement();
        writer.writeEndDocument();
        writer.close();

        String xmlString = baos.toString("UTF-8")

        data.put( Document.NON_JSON_CONTENT_KEY, xmlString );

        Document converted = new Document(doc.getId(), data);
        return converted;
    }

    public String getRequiredContentType()
    {
        return "application/ld+json";
    }

    public String getResultContentType()
    {
        return "text/xml";
    }
}
