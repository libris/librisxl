package whelk.converter

import whelk.Document
import whelk.JsonLd

import javax.xml.stream.XMLOutputFactory
import javax.xml.stream.XMLStreamWriter

/**
 * An extremely minimal and very lossy converter from jsonld to dublin core. Only identifiers are included.
 */
class JsonLD2DublinCoreConverter implements FormatConverter
{
    Map convert(Map originaldata, String id) {
        HashMap<String, String> data = new HashMap<String, String>()

        ByteArrayOutputStream baos = new ByteArrayOutputStream()
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance()
        XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(baos)
        writer.writeStartElement("oai_dc", "dc", "http://www.openarchives.org/OAI/2.0/oai_dc/")
        writer.writeNamespace("oai_dc", "http://www.openarchives.org/OAI/2.0/oai_dc/")
        writer.writeNamespace("dc", "http://purl.org/dc/elements/1.1/")
        writer.writeNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")
        writer.writeAttribute("http://www.w3.org/2001/XMLSchema-instance", "schemaLocation",
                "http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd")

        writer.writeStartElement("http://purl.org/dc/elements/1.1/", "identifier")
        writer.writeCharacters(Document.BASE_URI.resolve(id).toString())
        writer.writeEndElement()

        writer.writeEndElement()
        writer.writeEndDocument()
        writer.close()

        String xmlString = baos.toString("UTF-8")

        data.put(JsonLd.NON_JSON_CONTENT_KEY, xmlString)

        return data
    }

    public String getRequiredContentType() {
        return "application/ld+json"
    }

    public String getResultContentType() {
        return "text/xml"
    }
}
