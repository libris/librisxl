package whelk.plugin

import static org.apache.commons.lang3.StringEscapeUtils.escapeJava

import org.codehaus.jackson.map.ObjectMapper

class JsonLdToTurtle {

    def INDENT = "  "

    Writer writer
    Map context
    String base
    Map keys = [id: "@id", value: "@value", type: "@type", lang: "@language"]
    Map prefixes = [:]
    def uniqueBNodeSuffix = ""
    String bnodeSkolemBase = null

    JsonLdToTurtle(Map context, OutputStream outStream, String base=null) {
        this.context = context.context
        this.prefixes = context.prefixes
        this.base = base
        writer = new OutputStreamWriter(outStream, "UTF-8")
    }

    void write(String s) {
        writer.write(s)
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
        return "<${ref}>"
    }

    String toValidTerm(String term) {
        // TODO: only done to help the sensitive Sesame turtle parser..
        return term.replaceAll(/%/, /0/).replaceAll(/\./, '')
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

    def objectToTurtle(obj, level=0, viaKey=null) {
        def indent = INDENT * (level + 1)
        if (!(obj instanceof Map) || obj[keys.value]) {
            toLiteral(obj, viaKey)
            return Collections.emptyList()
        }
        def s = obj[keys.id]
        if (s && obj.size() > 1) {
            write(refRepr(s))
        } else if (level > 0) {
            writeln("[")
        } else {
            return Collections.emptyList()
        }
        def topObjects = []
        def first = true
        obj.each { key, vs ->
            def term = termFor(key)
            def revKey = (term == null)? revKeyFor(key) : null
            if (term == null && revKey == null)
                return
            if (term == keys.id || term == "@context")
                return
            vs = (vs instanceof List)? vs : vs != null? [vs] : []
            if (!vs) // TODO: && not @list
                return

            if (revKey) {
                vs.each {
                    def node = it.clone()
                    node[revKey] = [(keys.id): s]
                    topObjects << node
                }
            } else {
                if (!first) {
                    writeln(" ;")
                }
                first = false
                if (term == "@type") {
                    term = "a"
                    write(indent + term + " " + vs.collect { termFor(it) }.join(", "))
                    return
                }
                term = toValidTerm(term)
                write(indent + term + " ")
                vs.eachWithIndex { v, i ->
                    if (i > 0) write(" , ")
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
        if (level == 0) {
            writeln(" .")
            writeln()
            topObjects.each {
                objectToTurtle(it)
            }
            flush()
            return Collections.emptyList()
        } else {
            writeln()
            write(indent + "]")
            return topObjects
        }
    }

    def objectToTrig(String iri, Map obj) {
        writeln()
        writeln("GRAPH <$iri> {")
        writeln()
        objectToTurtle(obj)
        writeln("}")
        writeln()
    }

    void toLiteral(obj, viaKey=null) {
        def value = obj
        def lang = context["@language"]
        def datatype = null
        if (obj instanceof Map) {
            value = obj[keys.value]
            datatype = obj[keys.datatype]
        } else {
            def kdef = context[viaKey]
            def coerceTo = (kdef instanceof Map)? kdef["@type"] : null
            if (coerceTo == "@vocab") {
                write(refRepr(value, true))
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
        def serializer = new JsonLdToTurtle(context, bos, base)
        serializer.toTurtle(source)
        return bos
    }

    static void main(args) {
        def mapper = new ObjectMapper()
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
