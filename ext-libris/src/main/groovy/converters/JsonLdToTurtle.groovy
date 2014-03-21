package se.kb.libris.whelks.plugin

import org.codehaus.jackson.map.ObjectMapper

class JsonLdToTurtle {

    def INDENT = "  "

    PrintWriter pw
    Map context
    String base
    Map keys = [id: "@id", value: "@value", type: "@type", lang: "@language"]
    Map prefixes = [:]

    JsonLdToTurtle(Map context, OutputStream outStream, String base=null) {
        this.context = context.context
        this.prefixes = context.prefixes
        this.base = base
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

    String coerceFor(key) {
        def kdef = context[key]
        if (!(kdef instanceof Map))
            return null
        return kdef["@type"]
    }

    String refRepr(String ref, useVocab=false) {
        def cI = ref.indexOf(":")
        if (cI > -1) {
            def pfx = ref.substring(0, cI)
            if (pfx == "_") {
                return toValidTerm(ref)
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
        pw.flush()
    }

    void prelude() {
        prefixes.each { k, v ->
            pw.println("@prefix ${k}: <${v}> .")
        }
        if (base) {
            pw.println("@base <${base}> .")
        }
        pw.println()
    }

    def objectToTurtle(obj, level=0, viaKey=null) {
        def indent = INDENT * (level + 1)
        if (obj instanceof String || obj[keys.value]) {
            toLiteral(obj, viaKey)
            return Collections.emptyList()
        }
        def s = obj[keys.id]
        if (s && obj.size() > 1) {
            pw.println(refRepr(s))
        } else if (level > 0) {
            pw.println("[")
        } else {
            return Collections.emptyList()
        }
        def topObjects = []
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
                if (term == "@type") {
                    term = "a"
                    pw.println(indent + term + " " + vs.collect { termFor(it) }.join(", ") + " ;")
                    return
                }
                term = toValidTerm(term)
                pw.print(indent + term + " ")
                vs.eachWithIndex { v, i ->
                    if (i > 0) pw.print(" , ")
                    if (v instanceof Map && keys.id in v) {
                        topObjects << v
                        pw.print(refRepr(v[keys.id]))
                    } else {
                        topObjects.addAll(objectToTurtle(v, level + 1, key))
                    }
                }
                pw.println(" ;")
            }
        }
        if (level == 0) {
            pw.println(indent + ".")
            pw.println()
            topObjects.each {
                objectToTurtle(it)
            }
            return Collections.emptyList()
        } else {
            pw.print(indent + "]")
            return topObjects
        }
    }

    void toLiteral(obj, viaKey=null) {
        // TODO: coerce using term def
        def value = obj
        def lang = context["@language"]
        def datatype = null
        // TODO: context.language
        if (obj instanceof Map) {
            value = obj[keys.value]
            datatype = obj[keys.datatype]
        } else {
            def coerceTo = coerceFor(viaKey)
            if (coerceTo == "@vocab") {
                pw.println(refRepr(value, true))
                return
            } else if (coerceTo == "@id") {
                pw.println(refRepr(value))
                return
            } else if (coerceTo) {
                datatype = coerceTo
            }
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
        def multiple = args.length > 2
        def base = null
        for (path in args[1..-1]) {
            if (path.startsWith("http://")) {
                base = path
                continue
            }
            if (multiple) {
                println(); println "GRAPH <$path> {"; println()
            }
            if (base) {
                println "BASE <$base>"
            }
            def source = new File(path).withInputStream { mapper.readValue(it, Map) }
            println JsonLdToTurtle.toTurtle(context, source).toString("utf-8")
            if (multiple) {
                println "}"; println()
            }
        }
    }

}
