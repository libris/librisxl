package whelk.converter

import groovy.util.logging.Log4j2 as Log
import org.apache.jena.iri.IRI
import org.apache.jena.iri.IRIFactory

import static org.apache.commons.lang3.StringEscapeUtils.escapeJava
import static whelk.util.Jackson.mapper

@Log
class JsonLdToTurtle {

    def INDENT = "  "

    Writer writer
    Map context
    String base
    Map keys = [id: "@id", value: "@value", type: "@type", lang: "@language", graph: "@graph"]
    Map prefixes = [:]
    def uniqueBNodeSuffix = ""
    String bnodeSkolemBase = null
    boolean useGraphKeyword
    boolean markEmptyBnode
    String emptyMarker = '_:Nothing'
    static IRIFactory iriFactory = IRIFactory.iriImplementation()

    JsonLdToTurtle(Map context, OutputStream outStream, Map opts = null) {
        this(context, outStream, opts?.base, opts)
    }

    JsonLdToTurtle(Map context, OutputStream outStream, String base, Map opts = null) {
        this.context = context.context
        this.prefixes = context.prefixes
        this.base = base
        this.useGraphKeyword = opts?.useGraphKeyword == true
        this.markEmptyBnode = opts?.markEmptyBnode == true
        if ('owl' in prefixes) emptyMarker = 'owl:Nothing'
        this.setOutputStream(outStream)
    }

    void setOutputStream(OutputStream outStream) {
        writer = new OutputStreamWriter(outStream, "UTF-8")
    }

    void write(String s) {
        writer.write(s)
    }

    void write(Boolean s) {
        writer.write(s.toString())
    }

    void write(Number s) {
        writer.write(s.toString())
    }

    void writeln(String s) {
        write(s)
        writeln()
    }

    void writeln() {
        writer.write("\n")
    }

    void flush() {
        writer.flush()
    }

    String termFor(key) {
        if (key.startsWith("@")) {
            return key
        } else if (key.indexOf(":") > -1) {
            return key
        } else if (context.containsKey(key)) {
            def kdef = context[key]
            if (kdef == null)
                return null
            def term = null
            if (kdef instanceof Map) {
                term = kdef["@id"] ?: key
            } else {
                term = kdef
            }
            if (term == null)
                return null
            return (term.indexOf(":") == -1)? ":" + term : term
        } else {
            return ":" + key
        }
    }

    String revKeyFor(key) {
        def kdef = context[key]
        if (!(kdef instanceof Map))
            return null
        return kdef["@reverse"]
    }

    String refRepr(String ref, useVocab=false) {
        def cI = ref.indexOf(":")
        if (cI > -1) {
            def pfx = ref.substring(0, cI)
            if (pfx == "_") {
                def nodeId = ref + uniqueBNodeSuffix
                if (bnodeSkolemBase) {
                    ref = bnodeSkolemBase + nodeId.substring(2)
                } else {
                    return toValidTerm(nodeId)
                }
            } else if (context[pfx]) {
                return ref
            }
        } else if (useVocab && ref.indexOf("/") == -1) {
            return ":" + ref
        }
        ref = cleanIri(cleanValue(ref).replaceAll(/ /, '+'))
        return "<${ref}>"
    }

    String toValidTerm(String term) {
        term = cleanValue(term)
        if (term.indexOf('://') > -1) {
            return "<${term}>"
        }
        // TODO: hack to pseudo-fix problematic pnames...
        return term.
            replaceAll(/%/, /0/).
            replaceAll(/\./, '')
    }

    String cleanIri(String iriString) {
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
        return cleanedIriString
    }

    String cleanValue(String v) {
        return v.replaceAll("\\p{Cntrl}", '')
    }

    void toTrig(obj, id) {
        prelude()
        objectToTrig(id, obj)
        flush()
    }

    void toTurtle(obj) {
        prelude()
        objectToTurtle(obj)
        flush()
    }

    String genSkolemId() {
        return bnodeSkolemBase + UUID.randomUUID()
    }

    void prelude() {
        prefixes.each { k, v ->
            writeln("PREFIX ${k}: <${v}>")
        }
        if (base) {
            writeBase(base)
        }
        writeln()
        flush()
    }

    void writeBase(String iri) {
        writeln("BASE <$iri>")
    }

    boolean isListContainer(String term) {
        return context[term] instanceof Map &&
            context[term]['@container'] == '@list'
    }

    boolean isLangContainer(String term) {
        return context[term] instanceof Map &&
            context[term]['@container'] == '@language'
    }

    def objectToTurtle(obj, level=0, viaKey=null) {
        def indent = INDENT * (level + 1)

        if (isLangContainer(viaKey) && obj instanceof Map) {
            boolean first = true
            obj.each { lang, value ->
                def values = value instanceof List ? value : value != null ? [value] : []
                values.each {
                    if (!first) write(' , ')
                    toLiteral(
                            [(keys.value): it, (keys.lang): lang],
                            viaKey)
                    first = false
                }
            }
            return Collections.emptyList()
        }

        if (!(obj instanceof Map) || obj[keys.value]) {
            toLiteral(obj, viaKey)
            return Collections.emptyList()
        }

        boolean explicitList = '@list' in obj

        if (isListContainer(viaKey)) {
            obj = ['@list': obj]
        }

        def s = obj[keys.id]

        boolean isList = '@list' in obj
        boolean startedList = isList

        if (explicitList) {
            write('( ')
        }
        if (s && obj.size() > 1) {
            write(refRepr(s))
        } else if (level > 0) {
            if (!isList) {
                writeln("[")
            }
        } else {
            if (obj.containsKey(keys.graph)) {
                if (obj.keySet().any { it[0] != '@' }) {
                    // TODO: this is the default graph, described...
                    write('[]')
                }
            } else {
                return Collections.emptyList()
            }
        }

        def topObjects = []

        def first = true
        boolean endedList = false

        obj.each { key, vs ->
            def term = termFor(key)
            def revKey = (term == null)? revKeyFor(key) : null
            if (term == null && revKey == null)
                return
            if (term == keys.id || term == "@context")
                return
            vs = vs instanceof List ? vs : vs != null ? [vs] : []
            if (!vs) // TODO: && not @list
                return

            if (term == keys.graph) {
                topObjects += vs
                return
            }

            boolean inList = isList || isListContainer(key)

            if (revKey) {
                vs.each {
                    def node = it.clone()
                    node[revKey] = [(keys.id): s]
                    topObjects << node
                }
            } else {
                if (!first) {
                    if (startedList && !inList && !endedList) {
                        endedList = true
                        write(" )")
                    }
                    writeln(" ;")
                }
                first = false

                if (term == "@type") {
                    term = "a"
                    write(indent + term + " " + vs.collect {
                        toValidTerm(termFor(it))
                    }.join(", "))
                    return
                }

                if (term != '@list') {
                    term = toValidTerm(term)
                    write(indent + term + " ")
                }

                vs.eachWithIndex { v, i ->
                    if (inList) {
                        if (!startedList) {
                            write("(")
                            startedList = true
                        }
                        write(" ")
                    } else if (i > 0) {
                        write(" , ")
                    }
                    if (bnodeSkolemBase && v instanceof Map && !v[keys.id]) {
                        v[keys.id] = s = genSkolemId()
                    }
                    if (v instanceof Map && keys.id in v) {
                        topObjects << v
                        write(refRepr(v[keys.id]))
                    } else {
                        topObjects.addAll(objectToTurtle(v, level + 1, key))
                    }
                }
            }
        }

        if (explicitList || (!isList && startedList) && !endedList) {
            write(" )")
        }

        if (level == 0) {
            if (!first) {
                writeln(" .")
            }
            writeln()
            topObjects.each {
                objectToTurtle(it)
            }
            flush()
            return Collections.emptyList()
        } else {
            writeln()
            write(indent)
            if (!isList) {
                // NOTE: hack for e.g. BlazeGraph
                if (obj.size() == 0 && markEmptyBnode) {
                    writeln("a $emptyMarker")
                    write(indent)
                }
                write("]")
            }
            return topObjects
        }
    }

    def objectToTrig(String iri, Map obj) {
        writeln()
        if (useGraphKeyword) {
            write("GRAPH ")
        }
        writeln("<$iri> {")
        writeln()
        objectToTurtle(obj)
        writeln("}")
        writeln()
        flush()
    }

    void toLiteral(obj, viaKey=null) {
        def value = obj
        def lang = context["@language"]
        def datatype = null
        if (obj instanceof Map) {
            value = obj[keys.value]
            datatype = obj[keys.type]
            lang = obj[keys.lang]
        } else {
            def kdef = context[viaKey]
            def coerceTo = (kdef instanceof Map)? kdef["@type"] : null
            if (coerceTo == "@vocab") {
                write(value instanceof String ?
                        refRepr(value, true): value)
                return
            } else if (coerceTo == "@id") {
                write(refRepr(value))
                return
            } else if (coerceTo) {
                datatype = coerceTo
            } else {
                def termLang = (kdef instanceof Map)? kdef["@language"] : null
                if (kdef instanceof Map) {
                    lang = termLang
                }
            }
        }
        if (value instanceof String) {
            def escaped = escapeJava(value)
            write('"')
            write(escaped)
            write('"')
            if (datatype)
                write("^^" + termFor(datatype))
            else if (lang)
                write("@" + lang)
        } else { // boolean or number
            write(value.toString())
        }
    }

    static Map parseContext(Map src) {
        def context = [:]
        def cdef = src["@context"]
        if (cdef instanceof List) {
            cdef.each { context.putAll(it) }
        } else {
            context.putAll(cdef)
        }
        // TODO: override from context..
        def prefixes = [:]
        context.each { k, v ->
            if (v instanceof String && v =~ /\W$/) {
                prefixes[k == "@vocab"? "": k] = v
            }
        }
        return [context: context, prefixes: prefixes]
    }

    static OutputStream toTurtle(context, source, base=null) {
        def bos = new ByteArrayOutputStream()
        def opts = [base: base]
        def serializer = new JsonLdToTurtle(context, bos, opts)
        serializer.toTurtle(source)
        return bos
    }

    static OutputStream toTrig(context, source, base=null, String iri) {
        def bos = new ByteArrayOutputStream()
        def opts = [base: base]
        def serializer = new JsonLdToTurtle(context, bos, opts)
        serializer.toTrig(source, iri)
        return bos
    }

    static void main(args) {
        def contextSrc = new File(args[0]).withInputStream { mapper.readValue(it, Map) }
        def context = JsonLdToTurtle.parseContext(contextSrc)
        if (args.length == 2) {
            def source = new File(args[1]).withInputStream { mapper.readValue(it, Map) }
            println JsonLdToTurtle.toTurtle(context, source).toString("utf-8")
        } else {
            def serializer = new JsonLdToTurtle(context, System.out)
            serializer.prelude()
            for (path in args[1..-1]) {
                if (path.startsWith("http://")) {
                    serializer.writeBase(path)
                    continue
                }
                def source = new File(path).withInputStream { mapper.readValue(it, Map) }
                serializer.objectToTrig(path, source)
            }
        }
    }

}
