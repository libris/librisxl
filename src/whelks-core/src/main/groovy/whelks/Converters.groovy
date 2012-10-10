package se.kb.libris.conch.converter

import org.json.simple.*

import se.kb.libris.util.marc.Controlfield
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.Iso2709MarcRecordReader
import org.codehaus.jackson.map.ObjectMapper

/**
 *
 * A mechanical transcription of {@link MarcRecord}s into JSON. The result is
 * compliant with the <a href="http://dilettantes.code4lib.org/blog/category/marc-in-json/">MARC-in-JSON</a> JSON schema.
 */
class MarcJSONConverter {
    protected final static ObjectMapper mapper = new ObjectMapper();
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

    static String not_quite_so_old_toJSONString(MarcRecord record) {
        def json = new JSONObject()
        def fields = new JSONArray()

        record.fields.each {
            def field = new JSONObject()
            if (it instanceof Controlfield) {
                field.put(it.tag, it.data)
            } else {
                def datafield = new JSONObject()
                datafield.put("ind1", "" + it.getIndicator(0))
                datafield.put("ind2", "" + it.getIndicator(1))
                def subfields = new JSONArray()
                it.subfields.each {
                    def subfield = new JSONObject()
                    subfield.put(it.code, it.data);
                    subfields.add(subfield)
                }
                datafield.put("subfields", subfields)
                field.put(it.tag, datafield)
            }
            fields.add(field)
        }
        json.put("leader", record.leader)
        json.put("fields", fields)
        
        return json.toString()
    }

    static String toJSONString(MarcRecord record) {
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
                    subfield.put(Character.toString(it.code), it.data)
                    subfields.add(subfield)
                }
                datafield.put("subfields", subfields)
                field.put(it.tag, datafield)
            }
            fields.add(field)
        }
        json.put("leader", record.leader)
        json.put("fields", fields)
        return json.toString()
    }

    static void main(args) {
        MarcRecord record = new File(args[0]).withInputStream {
            new Iso2709MarcRecordReader(it/*, "utf-8"*/).readRecord()
        }
        println toJSONString(record)
        println "------------------------------------------------"
        println not_quite_so_old_toJSONString(record)/*.replaceAll(
                /(?m)\{\s+(\S+: "[^"]+")\s+\}/, '{$1}')*/
    }
}
