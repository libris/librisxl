package whelk.converter

import groovy.transform.CompileStatic

import trld.jsonld.Compaction
import trld.jsonld.Expansion
import trld.platform.Output
import trld.platform.Input
import trld.trig.Parser

@CompileStatic
class TrigToJsonLdParser {
    public static Map parse(InputStream inStream) {
        return parse(inStream, (Map) null);
    }

    public static Map parse(InputStream inStream, Map context) throws IOException {
        Map data = (Map) Parser.parse(new Input(inStream))
        return (Map) compact(data, context)
    }

    static Object expand(Object data, String baseIri=null) {
        Expansion.expand(data, baseIri)
    }

    static Object compact(Object data, Map context, String baseIri=null) {
        return Compaction.compact(context, expand(data, baseIri))
    }
}
