package whelk.converter

import java.text.Normalizer
import groovy.util.logging.Log4j2 as Log

import se.kb.libris.util.marc.Controlfield
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.Iso2709MarcRecordReader
import se.kb.libris.util.marc.io.MarcXmlRecordReader
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.ObjectNode


/**
 *
 * A mechanical transcription of {@link MarcRecord}s into JSON. The result is
 * compliant with the <a href="http://dilettantes.code4lib.org/blog/category/marc-in-json/">MARC-in-JSON</a> JSON schema.
 */
@Log
class MarcJSONConverter {

    protected final static ObjectMapper mapper = new ObjectMapper()

    static String old_toJSONString(MarcRecord record) {
        def builder = new groovy.json.JsonBuilder()
        builder {
            "leader"(record.leader)
            "fields"(record.fields.collect {[
                (it.tag): (it instanceof Controlfield)? (it.data) : [
                    ind1: it.getIndicator(0),
                    ind2:  it.getIndicator(1),
                    subfields: it.subfields.collect { [(it.code): it.data] }
                ]
            ]})
        }
        return builder.toString()
    }

    static String toJSONString(MarcRecord record) {
        return toObjectNode(record).toString()
    }

    static Map toJSONMap(MarcRecord record) {
        def node = toObjectNode(record)
        return mapper.readValue(node, Map)
    }

    private static ObjectNode toObjectNode(MarcRecord record) {
        def json = mapper.createObjectNode()
        def fields = mapper.createArrayNode()
        record.fields.each {
            def field = mapper.createObjectNode()
            if (it instanceof Controlfield) {
                field.put(it.tag, it.data)
            } else {
                def datafield = mapper.createObjectNode()
                datafield.put("ind1", "" + it.getIndicator(0))
                datafield.put("ind2", "" + it.getIndicator(1))
                def subfields = mapper.createArrayNode()
                it.subfields.each {
                    def subfield = mapper.createObjectNode()
                    subfield.put(Character.toString(it.code), it.data) //normalizeString(it.data))
                    subfields.add(subfield)
                }
                datafield.put("subfields", subfields)
                field.put(it.tag, datafield)
            }
            fields.add(field)
        }
        json.put("leader", record.leader)
        json.put("fields", fields)
        return json
    }

    static InputStream getNormalizedInputStreamFromFile(File f) {
        String unicodeString = f.getText("utf8")
        if (!Normalizer.isNormalized(unicodeString, Normalizer.Form.NFC)) {
            String newString = Normalizer.normalize(unicodeString, Normalizer.Form.NFC)
            return new ByteArrayInputStream(newString.getBytes("UTF-8"))
        }
        return f.newInputStream()
    }

    static void main(args) {
        /*
        MarcRecord record = new File(args[0]).withInputStream {
        new Iso2709MarcRecordReader(it).readRecord()
        }
        */
        MarcRecord record = null
        if (args.length > 1 && args[0] == "-xml")  {
            record = new MarcXmlRecordReader(getNormalizedInputStreamFromFile(new File(args[1]))).readRecord()
        }
        if (record == null) {
            record = new Iso2709MarcRecordReader(getNormalizedInputStreamFromFile(new File(args[0]))).readRecord()
        }
        println toJSONString(record)
        /*println not_quite_so_old_toJSONString(record)*//*.replaceAll(
            /(?m)\{\s+(\S+: "[^"]+")\s+\}/, '{$1}')*/
    }
}
