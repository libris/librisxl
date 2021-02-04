import whelk.util.DocumentUtil

selectBySqlWhere('deleted = false') { doc ->
    try {
        DocumentUtil.findKey(doc.graph, '@id') { String value, List path ->
            if (value.startsWith("https://libris") || value.startsWith("http://libris") || value.startsWith("https://id.kb.se")) {
                def p = path.findAll {!(it instanceof Integer) && it != '@id'}
                if (p) {
                    incrementStats('0 p', p.join('.'))
                    incrementStats(p.last(), p.join('.'))
                }
            }
        }    
    } catch (Exception e) {
        println(e)
    }
}