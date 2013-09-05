package se.kb.libris.whelks.plugin

import org.codehaus.jackson.map.ObjectMapper

class JsonLdToTurtle {

    def INDENT = "  "

    PrintWriter pw
    Map context
    Map keys = [id: "@id", value: "@value", type: "@type", lang: "@language"]
    Map prefixes = [:]

    JsonLdToTurtle(Map context, OutputStream outStream) {
        this.context = context.context
        this.prefixes = context.prefixes
        pw = new PrintWriter(outStream)
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
            def term = (kdef instanceof Map)? kdef["@id"] : kdef
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

    String refRepr(String ref) {
        return ref.startsWith("_:")? ref : "<${ref}>"
    }

    void toTurtle(obj) {
        prelude()
        objectToTurtle(obj)
        pw.flush()
    }

    void prelude() {
        prefixes.each { k, v ->
            pw.println("@prefix ${k}: <${v}> .")
        }
        pw.println()
    }

    void objectToTurtle(obj, level=0) {
        def indent = INDENT * (level + 1)
        if (obj instanceof String || obj[keys.value]) {
            toLiteral(obj)
            return
        }
        def s = obj[keys.id]
        if (s && obj.size() > 1) {
            pw.println(refRepr(s))
        } else if (level > 0) {
            pw.println("[")
        } else {
            return
        }
        def topObjects = []
        obj.each { key, vs ->
            def term = termFor(key)
            def revKey = (term == null)? revKeyFor(key) : null
            if (term == null && revKey == null)
                return
            if (term == keys.id || term == "@context")
                return
            vs = (vs instanceof List)? vs : vs? [vs] : []
            if (!vs) // TODO: && not @list
                return

            if (revKey) {
                vs.each {
                    def node = it.clone()
                    node[revKey] = [(keys.id): s]
                    topObjects << node
                }
            } else {
                if (term == "@type") {
                    term = "a"
                    pw.println(indent + term + " " + vs.collect { termFor(it) }.join(", ") + " ;")
                    return
                }
                term = term.replaceAll(/%/, /-/) // TODO: only done to help the Sesame turtle parser..
                pw.print(indent + term + " ")
                vs.eachWithIndex { v, i ->
                    if (i > 0) pw.print(" , ")
                    if (v instanceof Map && keys.id in v) {
                        topObjects << v
                        pw.print(refRepr(v[keys.id]))
                    } else {
                        objectToTurtle(v, level + 1)
                    }
                }
                pw.println(" ;")
            }
        }
        if (level == 0) {
            pw.println(indent + ".")
            pw.println()
        } else {
            pw.print(indent + "]")
        }
        topObjects.each {
            objectToTurtle(it)
        }
    }

    String toLiteral(obj) {
        // TODO: coerce using term def
        def value = obj
        def lang = context["@language"]
        def datatype = null
        // TODO: context.language
        if (obj instanceof Map) {
            value = obj[keys.value]
            datatype = obj[keys.datatype]
        }
        def escaped = value.replaceAll(/("|\\)/, /\\$1/)
        pw.print("\"${escaped}\"")
        if (lang)
            pw.print("@" + lang)
        else if (datatype)
            pw.print("^^" + termFor(datatype))
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

    static OutputStream toTurtle(context, source) {
        def bos = new ByteArrayOutputStream()
        def serializer = new JsonLdToTurtle(context, bos)
        serializer.toTurtle(source)
        return bos
    }

    static void main(args) {
        def mapper = new ObjectMapper()
        def source = new File(args[1]).withInputStream { mapper.readValue(it, Map) }
        def contextSrc = new File(args[0]).withInputStream { mapper.readValue(it, Map) }
        def context = JsonLdToTurtle.parseContext(contextSrc)
        println JsonLdToTurtle.toTurtle(context, source).toString("utf-8")
    }

}
