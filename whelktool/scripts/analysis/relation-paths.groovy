import whelk.util.DocumentUtil

selectBySqlWhere('deleted = false') { doc ->
    try {
        DocumentUtil.findKey(doc.graph, '@id') { String value, List path ->
            if (value.startsWith("https://libris") || value.startsWith("http://libris") || value.startsWith("https://id.kb.se")) {
                incrementStats('p', path.findAll {!(it instanceof Integer) && it != '@id'}.join('.'))
            }
        }    
    } catch (Exception e) {
        println(e)
    }
}