package whelk.util;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.postgresql.util.PSQLException;
import se.kb.libris.export.ExportProfile;
import se.kb.libris.util.marc.Datafield;
import se.kb.libris.util.marc.Field;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.Subfield;
import se.kb.libris.util.marc.io.MarcXmlRecordReader;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.converter.marc.JsonLD2MarcXMLConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

public class MarcExport {
    private static final Logger log = LoggerFactory.getLogger(MarcExport.class);

    public static Vector<MarcRecord> compileVirtualMarcRecord(ExportProfile profile, Document rootDocument,
                                                              Whelk whelk, JsonLD2MarcXMLConverter toMarcXmlConverter) {
        String bibXmlString = toXmlString(rootDocument, toMarcXmlConverter);
        if (bibXmlString == null)
            return null;

        MarcRecord bibRecord;
        try {
            bibRecord = MarcXmlRecordReader.fromXml(bibXmlString);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<String> authIds = new ArrayList<>();
        for (Datafield datafield : bibRecord.getDatafields()) {
            for (Subfield subfield : datafield.getSubfields("0")) {
                authIds.add(subfield.getData().replaceAll("#it", ""));
            }
        }

        Set<MarcRecord> auths = new HashSet<>();
        for (String authId : authIds) {
            Document authDoc = null;
            try {
                authDoc = getDocument(authId, whelk);
            } catch (PSQLException sqe) {
                // IGNORE.
                // The expected exception here is:
                // org.postgresql.util.PSQLException: ERROR: more than one row returned by a subquery used as an expression
                // See https://jira.kb.se/browse/LXL-1697
                log.warn("Failed to getDocument() an auth record with URI: {}. Ignoring.", authId);
            }
            if (authDoc != null) {
                String xmlString = toXmlString(authDoc, toMarcXmlConverter);
                if (xmlString != null) {
                    try {
                        auths.add(MarcXmlRecordReader.fromXml(xmlString));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            Iterator<MarcRecord> it = auths.iterator();
            while (it.hasNext()) {
                MarcRecord auth = it.next();
                if (auth.getFields("001").size() == 0)
                    it.remove();
            }
        }

        List<Document> holdingDocuments = whelk.getAttachedHoldings(rootDocument.getThingIdentifiers());
        TreeMap<String, MarcRecord> holdings = new TreeMap<>();

        for (Document holding : holdingDocuments) {
            try {
                holdings.put(holding.getHeldBySigel(), MarcXmlRecordReader.fromXml(toXmlString(holding, toMarcXmlConverter)));
            } catch (Exception e) {
                log.warn("Failed adding holding record when compiling MARC for " + rootDocument.getShortId(), e);
            }
        }

        // remove any existing 003
        ListIterator<Field> li = bibRecord.listIterator();
        while (li.hasNext())
            if (li.next().getTag().equals("003"))
                li.remove();

        try {
            profile.maybeAdd956Images(bibRecord, rootDocument);
        } catch (Exception e) {
            log.warn("Failed to insert images for: " + rootDocument.getShortId(), e);
        }

        try {
            return profile.mergeRecord(bibRecord, holdings, auths);
        } catch (Exception e) {
            log.warn("Failed to mangle marc record through profile: " + rootDocument.getShortId(), e);
            return null;
        }
    }

    /**
     * Get a document, on any valid ID/sameas for said document
     */
    public static Document getDocument(String idOrSameAs, Whelk whelk) throws PSQLException {
        String recordId = whelk.getStorage().getRecordId(idOrSameAs);
        if (recordId == null)
            return null;
        return whelk.loadEmbellished(whelk.getStorage().getSystemIdByIri(recordId));
    }

    /**
     * Make a marc xml string out of a whelk document
     */
    public static String toXmlString(Document doc, JsonLD2MarcXMLConverter toMarcXmlConverter) {
        try {
            return (String) toMarcXmlConverter.convert(doc.data, doc.getShortId()).get(JsonLd.NON_JSON_CONTENT_KEY);
        }
        catch (Exception | Error e) { // Depending on the converter, a variety of problems may arise here
            log.error("Conversion error for: " + doc.getCompleteId() + " cause: ", e);
            return null;
        }
    }
}
