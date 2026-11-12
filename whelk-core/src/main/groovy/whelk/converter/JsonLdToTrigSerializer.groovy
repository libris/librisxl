package whelk.converter

import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import groovy.util.logging.Slf4j as Log

import java.nio.charset.StandardCharsets
import whelk.util.Iris

import trld.platform.Output
import trld.trig.SerializerState
import trld.trig.Settings

@CompileStatic
class JsonLdToTrigSerializer {

    private CleanedTrigSerializerState state
    private WriterOutput out

    /**
     * Output is buffered; call flush() before closing or switching away from ostream.
     */
    JsonLdToTrigSerializer(Map context, OutputStream ostream) {
        def settings = new Settings()
        settings.useGraphKeyword = false
        settings.dropRdfstar = true // TODO: remove this line when we our Virtuoso supports supports this
        out = new WriterOutput(ostream)
        state = new CleanedTrigSerializerState(out, settings, context)
    }

    /**
     * Flushes any output buffered for the previous stream before switching.
     */
    void setOutputStream(OutputStream ostream) {
        flush()
        out = new WriterOutput(ostream)
        state.out = out
    }

    void flush() {
        out.flush()
    }

    /**
     * Sets the next number used for labelling generated blank nodes ("_:bnode-N").
     */
    void setBnodeCounter(int next) {
        state.bnodeCounter = next
    }

    void prelude() {
        state.prelude(state.prefixes)
    }

    void writeGraph(String id, Object data) {
        state.writeGraph(id, data)
    }

    static ByteArrayOutputStream toTrig(Object context, Object source, String base=null, String iri=null) {
        return serialize(context, source, base, new Settings())
    }

    static ByteArrayOutputStream toTurtle(Object context, Object source, String base=null) {
        boolean union = true
        Settings settings = new Settings(true, !union)
        return serialize(context, source, base, settings)
    }

    static ByteArrayOutputStream serialize(Object context, Object source, String base, Settings settings) {
        settings.dropRdfstar = true // TODO: remove this line when we our Virtuoso supports supports this
        Output out = new Output()
        CleanedTrigSerializerState state = new CleanedTrigSerializerState(out, settings, context, base)
        state.serialize(source)
        return out.getCaptured()
    }

}

/**
 * trld's Output prints each (typically tiny) string through a PrintStream, which locks and
 * flushes its encoder on every call. Write through a buffered Writer instead.
 */
@CompileStatic
class WriterOutput extends Output {
    private static final String LINE_SEPARATOR = System.lineSeparator()
    private final Writer writer

    WriterOutput(OutputStream ostream) {
        writer = new BufferedWriter(new OutputStreamWriter(ostream, StandardCharsets.UTF_8), 1 << 16)
    }

    @Override
    void write(String s) {
        writer.write(String.valueOf(s))
    }

    @Override
    void writeln(String s) {
        writer.write(String.valueOf(s))
        writer.write(LINE_SEPARATOR)
    }

    void flush() {
        writer.flush()
    }
}

@Log
@InheritConstructors
class CleanedTrigSerializerState extends SerializerState {

    // [8] IRIREF ::= '<' ([^#x00-#x20<>"{}|^`\] | UCHAR)* '>'
    // https://www.w3.org/TR/n-triples/#grammar-production-IRIREF
    // Also gets rid of some of the reserved URI characters (RFC 3986 2.2):
    // '[', ']', '@'. E.g. '[' and ']' are only allowed for identifying
    // IPv6 addresses. While these characters *can* be valid ("https://[2001:db8::1]",
    // "http://foo@bar:example.com"), their presence most likely indicates bad data.
    private static final java.util.regex.Pattern INVALID_IRI_CHARS = ~/[\x00-\x20<>"{}|^`\\\[\]@]/

    // TODO: We need to fix these when ENTERING the system (Validation/Normalization)!
    String refRepr(Object refobj) {
        if (refobj !instanceof String) {
            return refobj
        }

        if (refobj.startsWith('_:')) {
          return refobj
        }

        String iriString = refobj
        String cleanedIriString = INVALID_IRI_CHARS.matcher(iriString).replaceAll('')

        // Now catch things that are too broken to sensibly do anything about, e.g.,
        // "http://foo:", http://", etc.
        if (Iris.isBroken(cleanedIriString)) {
            cleanedIriString = "https://BROKEN-IRI/"
        }

        if (cleanedIriString != iriString) {
            log.warn("Broken IRI ${iriString}, changing to ${cleanedIriString}")
        }
        return super.refRepr(cleanedIriString)
    }

    // [144s] LANGTAG ::= '@' [a-zA-Z]+ ('-' [a-zA-Z0-9]+)*
    // https://www.w3.org/TR/turtle/#grammar-production-LANGTAG
    private static final java.util.regex.Pattern VALID_LANGTAG = ~/^[a-zA-Z]+(-[a-zA-Z0-9]+)*$/

    // TODO: Remove once data with invalid language tags (e.g. "9kd") has been cleaned up
    // (see https://github.com/libris/definitions/pull/639).
    @Override
    String toStr(Object v, String datatype, Object lang) {
        if (lang instanceof String && !((String) lang).isEmpty() && !VALID_LANGTAG.matcher((String) lang).matches()) {
            log.warn("Invalid language tag ${lang}, dropping it")
            lang = null
        }
        return super.toStr(v, datatype, lang)
    }

}
