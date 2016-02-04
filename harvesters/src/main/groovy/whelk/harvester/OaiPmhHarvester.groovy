package whelk.harvester

import groovy.util.logging.Slf4j as Log

import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamReader
import javax.xml.transform.TransformerFactory
import javax.xml.transform.stax.StAXSource
import javax.xml.transform.stream.StreamResult
import java.text.Normalizer

/**
 * Created by markus on 2016-02-03.
 */
@Log
class OaiPmhHarvester {

    static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"

    HarvestResult start(URL serviceURL, Format sourceFormat = Format.MARC, boolean acceptUpdates = true, boolean acceptDeletes = true) {

    }

    HarvestResult harvest(URL url, HarvestResult hdata) {
        try {
            XMLStreamReader streamReader = XMLInputFactory.newInstance().createXMLStreamReader(url.openStream())
            while (streamReader.hasNext()) {
                if (streamReader.isStartElement() && streamReader.localName == "record") {
                    OaiPmhRecord record = readRecord(streamReader)
                    println("Found record with id ${record.identifier} and data: ${record.record}")
                }
                if (streamReader.hasNext()) {
                    streamReader.next()
                }
            }

        } catch (IOException ioe) {
            log.error("Failed to read from URL $url")
            // Add proper error handling
        }
        return new HarvestResult(lastRecordDatestamp: new Date())
    }

    OaiPmhRecord readRecord(XMLStreamReader reader) {
        log.info("New record")

        OaiPmhRecord oair = new OaiPmhRecord()

        // Advance to header
        reader.nextTag()

        while (!endElement("header", reader)) {
            if (reader.isStartElement()) {
                switch (reader.localName) {
                    case "identifier":
                        oair.identifier = reader.elementText
                        break
                    case "datestamp":
                        oair.datestamp = Date.parse(DATE_FORMAT, reader.elementText)
                        break
                    case "setSpec":
                        oair.setSpecs << reader.elementText
                        break;
                    case "status":
                        oair.deleted = (reader.elementText == "deleted")
                        break;
                }
            }
            reader.next()
        }
        reader.next()

        // Advance to metadata
        reader.nextTag()

        if (reader.localName == "metadata") {
            reader.next()
            Writer outWriter = new StringWriter()
            TransformerFactory.newInstance().newTransformer().transform(new StAXSource(reader), new StreamResult(outWriter))
            oair.record = normalizeString(outWriter.toString())
        }
        return oair
    }

    private boolean endElement(String elementName, XMLStreamReader reader) {
        if (reader.isEndElement()) {
            return (reader.localName == elementName)
        }
        return false
    }

    String normalizeString(String inString) {
        if (!Normalizer.isNormalized(inString, Normalizer.Form.NFC)) {
            log.trace("Normalizing ...")
            return Normalizer.normalize(inString, Normalizer.Form.NFC)
        }
        return inString
    }

    enum Format {
        MARC,
        RDF
    }

}

