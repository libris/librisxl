package whelk.util

import groovy.util.logging.Log4j2
import se.kb.libris.export.ExportProfile
import se.kb.libris.util.marc.Field
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.MarcXmlRecordReader
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.converter.marc.JsonLD2MarcXMLConverter

@Log4j2
class MarcExport {
    static Vector<MarcRecord> compileVirtualMarcRecord(ExportProfile profile, Document rootDocument,
                                                       Whelk whelk, JsonLD2MarcXMLConverter toMarcXmlConverter) {
        String bibXmlString = toXmlString(rootDocument, toMarcXmlConverter)
        def xmlRecord = new XmlSlurper(false, false).parseText(bibXmlString)

        List auth_ids = []
        xmlRecord.datafield.subfield.each {
            if ( it.@code.text().equals("0") ) {
                auth_ids.add(it.text().replaceAll("#it", ""))
            }
        }

        def auths = new HashSet<MarcRecord>()
        auth_ids.each { String auth_id ->
            Document authDoc = getDocument(auth_id, whelk)
            if (authDoc != null) {
                String xmlString = toXmlString(authDoc, toMarcXmlConverter)
                if (xmlString != null)
                    auths.add(MarcXmlRecordReader.fromXml(xmlString))
            }
        }

        List<Document> holdingDocuments = whelk.storage.getAttachedHoldings(rootDocument.getThingIdentifiers())
        def holdings = new TreeMap<String, MarcRecord>()

        if (!profile.getProperty("holdtype", "NONE").equalsIgnoreCase("NONE")) {
            for (Document holding : holdingDocuments) {
                try {
                    holdings.put(holding.getSigel(), MarcXmlRecordReader.fromXml(toXmlString(holding, toMarcXmlConverter)))
                } catch (Exception e) {
                    log.warn("Failed adding holding record when compiling MARC for " + rootDocument.getShortId(), e)
                }
            }
        }

        MarcRecord bibRecord = MarcXmlRecordReader.fromXml(bibXmlString)

        // remove any existing 003
        ListIterator li = bibRecord.listIterator();
        while (li.hasNext())
            if (((Field)li.next()).getTag().equals("003"))
                li.remove()

        return profile.mergeRecord(bibRecord, holdings, auths)
    }

    /**
     * Get a document, on any valid ID/sameas for said document
     */
    static Document getDocument(String idOrSameAs, Whelk whelk) {
        String recordId = whelk.storage.getRecordId(idOrSameAs)
        if (recordId == null)
            return null
        return whelk.storage.loadDocumentByMainId(recordId)
    }

    /**
     * Make a marc xml string out of a whelk document
     */
    static String toXmlString(Document doc, JsonLD2MarcXMLConverter toMarcXmlConverter) {
        try {
            return (String) toMarcXmlConverter.convert(doc.data, doc.getShortId()).get(JsonLd.getNON_JSON_CONTENT_KEY())
        }
        catch (Exception | Error e) { // Depending on the converter, a variety of problems may arise here
            log.error(e)
            return null
        }
    }
}
