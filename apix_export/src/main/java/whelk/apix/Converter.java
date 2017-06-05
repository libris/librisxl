package whelk.apix;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.Iso2709Serializer;
import se.kb.libris.util.marc.io.MarcXmlRecordReader;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.converter.marc.JsonLD2MarcXMLConverter;
import whelk.converter.marc.MarcFrameConverter;
import whelk.filter.LinkFinder;

import java.io.ByteArrayInputStream;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import whelk.util.VCopyToWhelkConverter;

public class Converter
{
    private final Whelk m_whelk;
    private final JsonLd m_jsonld;
    private final JsonLD2MarcXMLConverter m_converter = new JsonLD2MarcXMLConverter();
    private MarcFrameConverter m_toJsonConverter;
    private final Logger s_logger = LogManager.getLogger(this.getClass());

    public Converter(Whelk whelk)
    {
        m_whelk = whelk;
        Map displayData = m_whelk.getDisplayData();
        Map vocabData = m_whelk.getVocabData();
        m_jsonld = new JsonLd(displayData, vocabData);
        LinkFinder lf = new LinkFinder(m_whelk.getStorage());
        m_toJsonConverter = new MarcFrameConverter(lf);
    }

    public String makeEmbellishedMarcJSONString(Document document, String collection)
    {
        // First embellish the document to restore a complete MARC record.
        List externalRefs = document.getExternalRefs();
        List convertedExternalLinks = JsonLd.expandLinks(externalRefs, (Map) m_jsonld.getDisplayData().get(JsonLd.getCONTEXT_KEY()));
        Map referencedData = m_whelk.bulkLoad(convertedExternalLinks);
        Map referencedData2 = new HashMap();
        for (Object key : referencedData.keySet())
            referencedData2.put(key, ((Document)referencedData.get(key)).data );
        m_jsonld.embellish(document.data, referencedData2);

        // Convert to MARCXML
        Map convertedData = m_converter.convert(document.data, document.getShortId());
        String convertedText = (String) convertedData.get(JsonLd.getNON_JSON_CONTENT_KEY());

        //convertedText = addEchoElimination(document, collection, convertedText);

        return convertedText;
    }

    private String addEchoElimination(Document document, String collection, String convertedText)
    {
        // Reconvert to JSONLD and calculate the checksum of the resulting JSONLD. Include said checksum in
        // 887 $a of the MARCXML we return. The point of this dance is that the reconvervsion-checksum can then be
        // compared with when we receive the echo of this save back from vcopy. If the two checksums are then identical
        // we know for a fact that we've received an echo (and not a genuine voyager change of the data) and that we can
        // disregard it.
        try
        {
            MarcXmlRecordReader marcXmlReader = new MarcXmlRecordReader(new ByteArrayInputStream(convertedText.getBytes("UTF-8")), "/record", null);
            MarcRecord marcRecord = marcXmlReader.readRecord();
            byte[] iso2709Binary = Iso2709Serializer.serialize(marcRecord);

            VCopyToWhelkConverter.VCopyDataRow row = new VCopyToWhelkConverter.VCopyDataRow();
            row.setData(iso2709Binary);
            row.setIsDeleted(false);
            row.setCreated(new Timestamp( ZonedDateTime.parse( document.getCreated(), DateTimeFormatter.ISO_ZONED_DATE_TIME).toInstant().getEpochSecond() * 1000L ));
            row.setUpdated(new Timestamp( ZonedDateTime.parse( document.getModified(), DateTimeFormatter.ISO_ZONED_DATE_TIME).toInstant().getEpochSecond() * 1000L ));
            row.setCollection(collection);
            switch (collection)
            {
                case "auth":
                    row.setAuth_id(Integer.parseInt(document.getControlNumber()));
                    break;
                case "bib":
                    row.setBib_id(Integer.parseInt(document.getControlNumber()));
                    break;
                case "hold":
                    row.setMfhd_id(Integer.parseInt(document.getControlNumber()));
                    break;
            }
            row.setAuthdata(null);
            row.setSigel(document.getSigel());

            ArrayList<VCopyToWhelkConverter.VCopyDataRow> rowList = new ArrayList<VCopyToWhelkConverter.VCopyDataRow>();
            rowList.add(row);
            Document reconvertedJson = (Document) VCopyToWhelkConverter.convert(rowList, m_toJsonConverter).get("document");
            String reconversionChecksum = reconvertedJson.getChecksum();

            // This is obviously a hackish way of editing XML, and it relies on the stability of the converter output.
            // But a full DOM parse + xpath + serialize again would likely cause even more problems.
            // This must be re-done in a more robust way if the decision to sync Voyager and XL in production is ever
            // revisited.
            String expected887beginning = "<subfield code=\"a\">{\"@id\":\"" + document.getShortId() + "\",";
            convertedText = convertedText.replace(expected887beginning, expected887beginning + "\"echo checksum\":\"" + reconversionChecksum + "\",");

            return convertedText;
        } catch (Throwable e)
        {
            s_logger.warn("Failed reconvert->checksum dance. 887 $a of " + document.getShortId() + " could not be set for echo-elimination.", e);
            return convertedText;
        }
    }
}
