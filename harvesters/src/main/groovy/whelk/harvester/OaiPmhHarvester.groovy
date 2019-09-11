package whelk.harvester

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log
import org.codehaus.jackson.map.ObjectMapper
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.MarcXmlRecordReader
import whelk.Changer
import whelk.Document
import whelk.Whelk
import whelk.converter.MarcJSONConverter
import whelk.converter.marc.MarcFrameConverter
import whelk.util.LegacyIntegrationTools

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
@CompileStatic
class OaiPmhHarvester {

    static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"
    static final String DEFAULT_SOURCE_SYSTEM = "xl"
    Whelk whelk
    MarcFrameConverter marcFrameConverter

    static final ObjectMapper mapper = new ObjectMapper()

    static final Map<Integer, String> MARCTYPE_COLLECTION = [
            (MarcRecord.AUTHORITY)    : "auth",
            (MarcRecord.BIBLIOGRAPHIC): "bib",
            (MarcRecord.HOLDINGS)     : "hold"
    ]


    OaiPmhHarvester() {}

    OaiPmhHarvester(Whelk w, MarcFrameConverter mfc) {
        whelk = w
        marcFrameConverter = mfc
    }

    synchronized HarvestResult harvest(String serviceUrl, String sourceSystem, String verb, String metadataPrefix, Date from = null, Date until = null) {
        harvest(serviceUrl, null, null, sourceSystem, verb, metadataPrefix, from, until)
    }

    synchronized HarvestResult harvest(String serviceURL, String username, String password, String sourceSystem, String verb, String metadataPrefix, Date from = null, Date until = null) {
        HarvestResult harvestResult = new HarvestResult(from, until, sourceSystem)
        authenticate(username, password)
        boolean harvesting = true

        URL url = constructRequestUrl(serviceURL, verb, metadataPrefix, null, harvestResult.fromDate, harvestResult.untilDate)

        try {
            while (harvesting == true) {
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
            log.debug "harvesting finished"
        } catch (RecordFromThePastException rftpe) {
            log.warn("Record ${rftpe.badRecord.identifier} has datestamp (${rftpe.badRecord.datestamp}) before requested (${harvestResult.fromDate}). URL used to retrieve results: ${url.toString()}")
        } catch (RecordFromTheFutureException rftfe) {
            log.warn("Record ${rftfe.badRecord.identifier} has datestamp (${rftfe.badRecord.datestamp}) after requested (${harvestResult.untilDate}).")
        } catch (IOException ioe) {
            log.error("Failed to read from URL $url")
            throw ioe
        } catch (BrokenRecordException bre) {
            log.error(bre.message)
            throw bre
        } catch (Exception e) {
            log.error("Some other error:", e)
            throw e
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

        String incomingCollection = null

        try {
            xmlInputStream = url.openStream()
            streamReader = XMLInputFactory.newInstance().createXMLStreamReader(xmlInputStream)
            while (streamReader.hasNext()) {
                if (streamReader.isStartElement() && streamReader.localName == "record") {
                    OaiPmhRecord record = readRecord(streamReader)
                    String collection = addRecord(record, hdata, documentList)
                    if (collection != null)
                        incomingCollection = collection;
                }
                if (streamReader.hasNext()) {
                    streamReader.next()
                }
                if (streamReader.isStartElement() && streamReader.localName == "resumptionToken") {
                    hdata.resumptionToken = streamReader.elementText
                }
            }
        }
        finally {
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
        String sourceSystem = hdata.sourceSystem == null ? DEFAULT_SOURCE_SYSTEM : hdata.sourceSystem
        try {

            if (documentList.count { it } > 0)
                whelk.bulkStore(documentList, sourceSystem, Changer.unknown(), incomingCollection)
            else
                log.debug("documentList contains no records")

        } catch (any) {
            log.error("bulkstorfel", any)
        }
        log.debug("Done reading stream. Documents still in documentList: ${documentList.size()}")
        log.debug("Imported ${hdata.numberOfDocuments}. Last timestamp: ${hdata.lastRecordDatestamp}. Number deleted: ${hdata.numberOfDocumentsDeleted}")
        return hdata
    }

    OaiPmhRecord readRecord(XMLStreamReader reader) {
        log.trace("New record")

        OaiPmhRecord oair = new OaiPmhRecord()

        // Advance to header
        reader.nextTag()
        String status = reader.getAttributeValue(null, "status")
        log.trace "Status: ${status}"
        oair.deleted = (status == "deleted")

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

    final String addRecord(OaiPmhRecord record, HarvestResult hdata, List<Document> docs) {

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
            return null
        }

        log.trace("Found record with id ${record.identifier} and data: ${record.record}")
        String collection = null
        if (record.deleted) {
            String systemId = null // FIXME locate does not exist - String systemId = whelk.storage.locate(record.identifier, false)?.id
            if (systemId) {
                MarcRecord marcRecord = MarcXmlRecordReader.fromXml(record.record)
                log.debug("Delete request for ${record.identifier}. " +
                        "Located in system as ${systemId}. " +
                        "Collection is: ${getCollection(marcRecord)}")
                try {
                    //TODO: Do not hard code ChangedIn parameter
                    whelk.remove(systemId, 'voyager', Changer.unknown())
                } catch (all) {
                    log.error("Could not remove record with ID ${record.identifier}. " +
                            "Located in system as ${systemId}. " +
                            "Collection is: ${getCollection(marcRecord)}", all)
                }
                hdata.numberOfDocumentsDeleted++
            }
        } else {
            def documentAndCollection = createDocument(record)
            if (documentAndCollection) {
                Document doc = documentAndCollection[0] as Document
                collection = documentAndCollection[1]
                docs << doc
                hdata.numberOfDocuments++
            } else {
                hdata.numberOfDocumentsSkipped++
            }
            if (docs.size() > 0 && docs.size() % 1000 == 0) {
                String sourceSystem = hdata.sourceSystem == null ? DEFAULT_SOURCE_SYSTEM : hdata.sourceSystem
                log.debug "adding ${docs.count { it }} documents to whelk"
                whelk.bulkStore(docs, sourceSystem, Changer.unknown(), collection)
                docs = []
            }
        }
        return collection
    }

    /**
     * This is the method to override when checking if a document should be processed for saving and/or deletion
     * @param oaiPmhRecord
     * @return true if ok
     */
    boolean okToSave(OaiPmhRecord oaiPmhRecord) {
        true
    }

    String getCollection(MarcRecord marcRecord) {
        return MARCTYPE_COLLECTION[marcRecord.type]
    }

    List createDocument(OaiPmhRecord oaiPmhRecord) {

        if (oaiPmhRecord.record == null) {
            return null
        }
        if (oaiPmhRecord.format == Format.MARC) {

            MarcRecord marcRecord = MarcXmlRecordReader.fromXml(oaiPmhRecord.record)

            String collection = getCollection(marcRecord)

            String recordId = "/" + collection + "/" + marcRecord.getControlfields("001").get(0).getData()

            String originalIdentifier = null
            log.trace("Start check for 887")
            try {
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
                    long marcRecordModified = getMarcRecordModificationTime(marcRecord)?.getTime()
                    log.debug("record timestamp: $marcRecordModified")
                    log.debug("    xl timestamp: $originalModified")
                    long diff = ((marcRecordModified - originalModified) / 1000) as long
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

            //doc.setId(originalIdentifier)
            String mainId;
            if (originalIdentifier != null)
                mainId = originalIdentifier
            else
                mainId = LegacyIntegrationTools.generateId(recordId)

            Map<String, List> extraData = [:]
            for (spec in oaiPmhRecord.setSpecs) {
                List setSpecs = extraData.get("oaipmhSetSpecs", [])
                setSpecs.add(spec.toString())
            }

            Map jsonMap = MarcJSONConverter.toJSONMap(marcRecord)
            try {
                Map converted = marcFrameConverter.convert(jsonMap, mainId, extraData)
                Document doc = new Document(converted)

                doc.addRecordIdentifier(Document.BASE_URI.resolve(recordId).toString())
                log.trace("Created document with ID ${doc.id}")
                return [doc, collection]

            }
            catch (all) {
                println all.message
                //println all.stackTrace
                println "String representation of record:   ${jsonMap.inspect()}"
                return null
            }


        } else {
            log.error("Non-MARC post harvested, not yet supported!")
            return null
            // TODO: Make JSONLD document from RDF/XML
        }
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
            return Date.parse("yyyyMMddHHmmss.S", datetime)
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

