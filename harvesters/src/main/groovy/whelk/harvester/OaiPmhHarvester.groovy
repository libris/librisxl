package whelk.harvester

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.MarcXmlRecordReader
import whelk.Document
import whelk.Whelk
import whelk.converter.marc.MarcFrameConverter
import whelk.util.LegacyIntegrationTools
import whelk.converter.MarcJSONConverter

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
    Whelk whelk
    MarcFrameConverter marcFrameConverter

    static final ObjectMapper mapper = new ObjectMapper()

    static final Map<Integer, String> MARCTYPE_COLLECTION = [
            (MarcRecord.AUTHORITY) : "auth",
            (MarcRecord.BIBLIOGRAPHIC) : "bib",
            (MarcRecord.HOLDINGS) : "hold"
    ]


    OaiPmhHarvester() {}

    OaiPmhHarvester(Whelk w, MarcFrameConverter mfc) {
        whelk = w
        marcFrameConverter = mfc
    }

    synchronized HarvestResult harvest(String serviceUrl, String verb, String metadataPrefix, Date from = null, Date until = null) {
        harvest(serviceUrl, null, null, verb, metadataPrefix, from, until)
    }

    synchronized HarvestResult harvest(String serviceURL, String username, String password, String verb, String metadataPrefix, Date from = null, Date until = null) {
        HarvestResult harvestResult = new HarvestResult(from, until)
        authenticate(username, password)
        boolean harvesting = true

        URL url = constructRequestUrl(serviceURL, verb, metadataPrefix, null, harvestResult.fromDate, harvestResult.untilDate)

        try {
            while (harvesting) {
                harvestResult = readUrl(url, harvestResult)
                if (harvestResult.resumptionToken) {
                    log.debug("Found resumption token ${harvestResult.resumptionToken}")
                    url = constructRequestUrl(serviceURL, verb, metadataPrefix, harvestResult.resumptionToken)
                    // reset resumption token
                    harvestResult.resumptionToken = null
                } else {
                    harvesting = false
                }
            }
        } catch (RecordFromThePastException rftpe) {
            log.warn("Record ${rftpe.badRecord.identifier} has datestamp (${rftpe.badRecord.datestamp}) before requested (${harvestResult.fromDate}). URL used to retrieve results: ${url.toString()}")
        } catch (RecordFromTheFutureException rftfe) {
            log.warn("Record ${rftfe.badRecord.identifier} has datestamp (${rftfe.badRecord.datestamp}) after requested (${harvestResult.untilDate}).")
        } catch (BrokenRecordException bre) {
            log.error(bre.message)
        } catch (IOException ioe) {
            log.error("Failed to read from URL $url")
            // TODO: Add proper error handling
        } catch (Exception e) {
            log.error("Some other error: ${e.message}", e)
        }
        return harvestResult
    }

    URL constructRequestUrl(String baseUrl, String verb, String metadataPrefix, String resumptionToken, Date from = null, Date until = null) {
        StringBuilder queryString = new StringBuilder("?verb=" + URLEncoder.encode(verb, "UTF-8"))
        if (resumptionToken) {
            queryString.append("&resumptionToken=" + URLEncoder.encode(resumptionToken, "UTF-8"))
        } else {
            queryString.append("&metadataPrefix=" + URLEncoder.encode(metadataPrefix, "UTF-8"))
            if (from && from.getTime() > 0) {
                queryString.append("&from=" + from.format(DATE_FORMAT, TimeZone.getTimeZone('UTC')))
            }
            if (until) {
                queryString.append("&until=" + until.format(DATE_FORMAT, TimeZone.getTimeZone('UTC')))
            }
        }

        URL url = new URL(baseUrl + queryString.toString())
        log.debug("Constructed URL: ${url.toString()}")

        return url
    }

    HarvestResult readUrl(URL url, HarvestResult hdata) {
        log.debug("Starting harvest. Last record datestamp: ${hdata.lastRecordDatestamp}")
        List<Document> documentList = new ArrayList<Document>()
        InputStream xmlInputStream
        XMLStreamReader streamReader
        try {
            xmlInputStream = url.openStream()
            streamReader = XMLInputFactory.newInstance().createXMLStreamReader(xmlInputStream)
            while (streamReader.hasNext()) {
                if (streamReader.isStartElement() && streamReader.localName == "record") {
                    OaiPmhRecord record = readRecord(streamReader)
                    documentList = addRecord(record, hdata, documentList)
                }
                if (streamReader.hasNext()) {
                    streamReader.next()
                }
                if (streamReader.isStartElement() && streamReader.localName == "resumptionToken") {
                    hdata.resumptionToken = streamReader.elementText
                }
            }
        } finally {
            log.trace("Closing streams")
            try {
                streamReader.close()
                xmlInputStream.close()
                log.trace("Streams successfully closed.")
            } catch (NullPointerException npe) {
                log.trace("Got npe on closing.")
            }
        }
        // Store remaining documents
        whelk.bulkStore(documentList)
        log.debug("Done reading stream. Documents still in documentList: ${documentList.size()}")
        log.debug("Imported ${hdata.numberOfDocuments}. Last timestamp: ${hdata.lastRecordDatestamp}. Number deleted: ${hdata.numberOfDocumentsDeleted}")
        return hdata
    }

    OaiPmhRecord readRecord(XMLStreamReader reader) {
        log.trace("New record")

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
                        oair.setDatestamp(reader.elementText)
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

        // Advance to metadata
        reader.nextTag()

        if (reader.localName == "metadata") {
            reader.nextTag() // Advance to record
            oair.setFormat(reader.namespaceURI)
            try {
                Writer outWriter = new StringWriter()
                TransformerFactory.newInstance().newTransformer().transform(new StAXSource(reader), new StreamResult(outWriter))
                oair.record = normalizeString(outWriter.toString())
            } catch (IllegalStateException ise) {
                throw new BrokenRecordException(oair.identifier)
            }
        }
        return oair
    }

    List<Document> addRecord(OaiPmhRecord record, HarvestResult hdata, List<Document> docs) {

        // Sanity check dates
        log.trace("Record date: ${record.datestamp}. Last record date: ${hdata.lastRecordDatestamp}")
        if (hdata.fromDate && record.datestamp.before(hdata.fromDate)) {
            log.error("I think ${record.savedDateString} is from the past.")
            throw new RecordFromThePastException(record)
        }
        if (hdata.untilDate && record.datestamp.after(hdata.untilDate)) {
            throw new RecordFromTheFutureException(record)
        }

        // Update lastRecordDatestamp
        hdata.lastRecordDatestamp = (record.datestamp.after(hdata.lastRecordDatestamp) ? record.datestamp : hdata.lastRecordDatestamp)

        if (!okToSave(record)) {
            hdata.numberOfDocumentsSkipped++
            return docs
        }

        log.trace("Found record with id ${record.identifier} and data: ${record.record}")
        if (record.deleted) {
            String systemId = whelk.storage.locate(record.identifier)?.id
            if (systemId) {
                log.debug("Delete request for ${record.identifier}. Located in system as ${systemId}.")
                whelk.remove(systemId)
                hdata.numberOfDocumentsDeleted++
            }
        } else {
            Document doc = createDocument(record)
            if (doc) {
                docs << doc
                hdata.numberOfDocuments++
            } else {
                hdata.numberOfDocumentsSkipped++
            }
            if (docs.size() % 1000 == 0) {
                whelk.bulkStore(docs)
                docs = []
            }
        }
        return docs
    }

    /**
     * This is the method to override when checking if a document should be processed for saving and/or deletion
     * @param oaiPmhRecord
     * @return true if ok
     */
    boolean okToSave(OaiPmhRecord oaiPmhRecord) {
        true
    }

    Document createDocument(OaiPmhRecord oaiPmhRecord) {
        Document doc
        if (oaiPmhRecord.record == null) {
            return null
        }
        if (oaiPmhRecord.format == Format.MARC) {
            MarcRecord marcRecord = MarcXmlRecordReader.fromXml(oaiPmhRecord.record)
            String collection = MARCTYPE_COLLECTION[marcRecord.type]

            String recordId = "/" + collection + "/" + marcRecord.getControlfields("001").get(0).getData()

            def manifest = [(Document.ID_KEY): LegacyIntegrationTools.generateId(recordId),
                            (Document.COLLECTION_KEY): collection,
                            (Document.CONTENT_TYPE_KEY): "application/x-marc-json"]

            log.trace("Start check for 887")
            try {
                String originalIdentifier = null
                long originalModified = 0

                for (field in marcRecord.getDatafields("887")) {
                    if (!field.getSubfields("2").isEmpty() && field.getSubfields("2").first().data == "librisxl") {
                        try {
                            def xlData = mapper.readValue(field.getSubfields("a").first().data, Map)
                            originalIdentifier = xlData.get("@id")
                            originalModified = xlData.get("modified") as long
                        } catch (Exception e) {
                            log.error("Failed to parse 887 as json for ${recordId}")
                        }
                    }
                }
                if (originalIdentifier) {
                    log.debug("Detected an original Libris XL identifier in Marc data: ${originalIdentifier}, updating manifest.")
                    manifest.put(Document.ID_KEY, originalIdentifier)
                    long marcRecordModified = getMarcRecordModificationTime(marcRecord)?.getTime()
                    log.debug("record timestamp: $marcRecordModified")
                    log.debug("    xl timestamp: $originalModified")
                    long diff = (marcRecordModified - originalModified) / 1000
                    log.debug("update time difference: $diff secs.")
                    if (diff < 30) {
                        log.debug("Record probably not edited in Voyager. Skipping ...")
                        return null
                    }
                }
            } catch (NoSuchElementException nsee) {
                log.trace("Record doesn't have a 887 field.")
            }
            log.trace("887 check complete.")

            def extraData = [:]

            for (spec in oaiPmhRecord.setSpecs) {
                extraData.get("oaipmhSetSpecs", []).add(spec.toString())
            }
            if (extraData) {
                manifest['extraData'] = extraData
            }

            doc = new Document(
                    MarcJSONConverter.toJSONMap(marcRecord),
                    manifest
                ).addIdentifier(Document.BASE_URI.resolve(recordId).toString())

            doc = marcFrameConverter.convert(doc)
            doc.data["controlNumber"] = marcRecord.getControlfields("001").get(0).getData()
        } else {
            // TODO: Make JSONLD document from RDF/XML
        }
        log.trace("Created document with ID ${doc.id}")
        return doc
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

    Date getMarcRecordModificationTime(MarcRecord record) {
        String datetime = record.getControlfields("005")?.get(0).getData()
        if (datetime) {
            return Date.parse("yyyyMMddHHmmss.S",datetime)
        }
        return null
    }

    protected void authenticate(String username, String password) {
        if (username && password) {
            log.trace("Setting username (${username}) and password (${password})")
        } else {
            log.trace("Not setting username/password")
        }
        try {
            Authenticator.setDefault(new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, password.toCharArray())
                }
            });
        } catch (Exception ex) {
            log.error("Exception getting authentication credentials: $ex")
        }
    }



    class OaiPmhRecord {
        String record
        String identifier
        private Date datestamp
        String savedDateString = null

        Format format = Format.MARC

        List<String> setSpecs = new ArrayList<String>()
        boolean deleted = false

        public Date getDatestamp() {
            return new Date(datestamp.getTime())
        }

        void setDatestamp(String dateString) {
            savedDateString = dateString
            datestamp = Date.parse(DATE_FORMAT, dateString)
        }

        void setFormat(String xmlNameSpace) {
            switch (xmlNameSpace) {
                case "http://www.loc.gov/MARC21/slim":
                    format = Format.MARC
                    break
                case "http://www.w3.org/1999/02/22-rdf-syntax-ns#":
                    format = Format.RDF
                    break
            }
        }
    }

    class XmlParsingFailedException extends RuntimeException {
        XmlParsingFailedException(String message) {
            super(message)
        }
    }

    class BrokenRecordException extends XmlParsingFailedException {
        String brokenId
        BrokenRecordException(String identifier) {
            super("Record ${identifier} has broken metadata")
            brokenId = identifier
        }
    }

    class DateInconsistencyException extends RuntimeException {
        OaiPmhRecord badRecord

        DateInconsistencyException(String message) {
            super(message)
        }
    }

    class RecordFromThePastException extends DateInconsistencyException {

        RecordFromThePastException(OaiPmhRecord record) {
            super("Record ${record.identifier} has datestamp ${record.datestamp}, occuring before requested date range.")
            badRecord = record
        }
    }

    class RecordFromTheFutureException extends DateInconsistencyException {

        RecordFromTheFutureException(OaiPmhRecord record) {
            super("Record ${record.identifier} has datestamp from the future (${record.datestamp})")
            badRecord = record
        }
    }

    enum Format {
        MARC,
        RDF
    }
}

