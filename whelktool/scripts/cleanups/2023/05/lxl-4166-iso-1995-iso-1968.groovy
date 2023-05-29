import whelk.util.DocumentUtil

def where = """
    (collection = 'bib' OR collection = 'hold' OR collection = 'auth')
    AND deleted = false
    AND modified > '2023-01-01'
"""

selectBySqlWhere(where) { doc ->
    DocumentUtil.traverse(doc.graph) { value, path ->
        if (path && ((String) path.last()).endsWith('ByLang') && value instanceof Map) {
            var iso1995 = value.keySet().findAll { it.contains('iso-1995') && !it.contains('kk-Latn-t-kk-Cyrl') }

            iso1995.each { String langTag ->
                incrementStats('Replaced', langTag)
                value.put(langTag.replace('1995', '1968'), value[langTag])
                value.remove(langTag)
                doc.scheduleSave()
            }
            return DocumentUtil.NOP
        }
    }
}
