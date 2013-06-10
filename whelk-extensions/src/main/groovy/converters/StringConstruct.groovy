package se.kb.libris.whelks.plugin

class StringConstruct {
    Map construct
    StringConstruct(construct) {
        this.construct = construct
    }
    String expand(Map data) {
        return expand(construct, data)
    }
    static String expand(def construct, Map data) {
        if (construct instanceof List)
            return expandList("", construct, data)
        def sb = new StringBuilder()
        construct.each { joiner, items ->
            if (items.is(true))
                sb << joiner
            else
                sb << expandList(joiner, items, data)
        }
        return sb.toString()
    }
    static String expandList(String joiner, List items, Map data) {
        def values = []
        def strFormat = false
        def required = 0
        def i = 0
        for (key in items) {
            if (key instanceof Number) {
                strFormat = true
                required = key
                continue
            }
            def v = key instanceof String? data[key] : expand(key, data)
            if (v) {
                values << v
                i++
            } else if (strFormat) {
                values << ""
            }
        }
        if (!strFormat)
            return values.join(joiner)
        if (required > i)
            return ""
        return String.format(joiner, values as String[])
    }
}
