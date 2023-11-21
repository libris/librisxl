package whelk.converter

import groovy.transform.CompileStatic

import trld.jsonld.Compaction
import trld.jsonld.Expansion
import trld.platform.Output
import trld.platform.Input
import trld.trig.Parser

@CompileStatic
class TrigToJsonLdParser {
    static Map parse(InputStream ins) {
        return (Map) Parser.parse(new Input(ins))
    }

    static Object expand(Object data, String baseIri=null) {
        Expansion.expand(data, baseIri)
    }

    static Object compact(Object data, Map context, String baseIri=null) {
        return Compaction.compact(context, expand(data, baseIri))
    }
}
