package se.kb.libris.conch.converter

import se.kb.libris.util.marc.Controlfield
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.Iso2709Deserializer
import se.kb.libris.util.marc.io.StrictIso2709Reader

class MarcJSONConverter {

    static String toJSONString(MarcRecord record) {
        def builder = new groovy.json.JsonBuilder()
        builder {
            "leader" record.leader
            for (f in record.fields) {
                if (f instanceof Controlfield) {
                    "$f.tag" f.data
                } else {
                    "$f.tag"(
                        [0, 1].collect { f.getIndicator(it) }
                        + f.subfields.collect { [(it.code): it.data] }
                    )
                }
            }
        }
        return builder.toPrettyString()
    }

    static void main(args) {
        byte[] barr = new File(args[0]).withInputStream {
            new StrictIso2709Reader(it).readIso2709()
        }
        MarcRecord record = Iso2709Deserializer.deserialize(barr)//, "utf-8")
        println toJSONString(record)
    }

}
