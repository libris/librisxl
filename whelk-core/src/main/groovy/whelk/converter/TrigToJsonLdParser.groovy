package whelk.converter

import groovy.transform.CompileStatic

import trld.Output
import trld.Input
import trld.trig.Parser

@CompileStatic
class TrigToJsonLdParser {
    static Map parse(InputStream ins) {
        return (Map) Parser.parse(new Input(ins))
    }
}
