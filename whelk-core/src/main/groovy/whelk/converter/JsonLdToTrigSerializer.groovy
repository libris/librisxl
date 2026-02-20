package whelk.converter

import groovy.transform.InheritConstructors
import groovy.util.logging.Log4j2 as Log

import org.apache.jena.iri.IRI
import org.apache.jena.iri.IRIFactory

import trld.platform.Output
import trld.trig.SerializerState
import trld.trig.Settings

class JsonLdToTrigSerializer {

    private CleanedTrigSerializerState state

    JsonLdToTrigSerializer(Map context, OutputStream ostream) {
        def settings = new Settings()
        settings.useGraphKeyword = false
        def out = new Output(new PrintStream(ostream))
        state = new CleanedTrigSerializerState(out, settings, context)
    }

    void setOutputStream(OutputStream ostream) {
        state.out = new Output(new PrintStream(ostream))
    }

    void prelude() {
        state.prelude(state.prefixes)
    }

    void writeGraph(String id, Object data) {
        state.writeGraph(id, data)
    }

    static OutputStream toTrig(context, source, base=null, String iri=null) {
        return serialize(context, source, base, new Settings())
    }

    static OutputStream toTurtle(context, source, base=null) {
        boolean union = true
        def settings = new Settings(true, !union)
        return serialize(context, source, base, settings)
    }

    static OutputStream serialize(context, source, base, settings) {
        def out = new Output()
        def state = new CleanedTrigSerializerState(out, settings, context, base)
        state.serialize(source)
        return out.getCaptured()
    }

}

@Log
@InheritConstructors
class CleanedTrigSerializerState extends SerializerState {

    static IRIFactory iriFactory = IRIFactory.iriImplementation()

    // TODO: We need to fix these when ENTERING the system (Validation/Normalization)!
    String refRepr(Object refobj) {
        if (refobj !instanceof String) {
            return refobj
        }

        if (refobj.startsWith('_:')) {
          return refobj
        }

        String iriString = refobj
        // [8] IRIREF ::= '<' ([^#x00-#x20<>"{}|^`\] | UCHAR)* '>'
        // https://www.w3.org/TR/n-triples/#grammar-production-IRIREF
        // Also gets rid of some of the reserved URI characters (RFC 3986 2.2):
        // '[', ']', '@'. E.g. '[' and ']' are only allowed for identifying
        // IPv6 addresses. While these characters *can* be valid ("https://[2001:db8::1]",
        // "http://foo@bar:example.com"), their presence most likely indicates bad data.
        String cleanedIriString = iriString.replaceAll(/[\x00-\x20<>"{}|^`\\\[\]@]/, '')

        // Now catch things that are too broken to sensibly do anything about, e.g.,
        // "http://foo:", http://", etc.
        IRI iri = iriFactory.create(cleanedIriString)
        if (iri.hasViolation(false)) { // false = ignore warnings, care only about errors
            cleanedIriString = "https://BROKEN-IRI/"
        }

        if (cleanedIriString != iriString) {
            log.warn("Broken IRI ${iri}, changing to ${cleanedIriString}")
        }
        return super.refRepr(cleanedIriString)
    }

}
