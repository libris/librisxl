import whelk.util.DocumentUtil

List<Map> genreForms = [['oldgf': 'xv8cln7g41q4n6h', 'saogf': 'gdsw32q030cdkdm', 'barngf': 'vs68cfjd2vlfkkx'],
                        ['oldgf': 'wt7bkm6f11w8wkq', 'saogf': 'xv8ckc8g43hck13', 'barngf': 'rp369s594r07gsb']]

genreForms.each { gf ->
    String where = "id in (SELECT id FROM lddb__dependencies WHERE dependsonid = '${gf['oldgf']}')"
    String oldgfUri
    String saogfUri
    String barngfUri

    selectByIds([gf['oldgf']]) {
        oldgfUri = it.graph[1]['@id']
    }
    selectByIds([gf['saogf']]) {
        saogfUri = it.graph[1]['@id']
    }
    selectByIds([gf['barngf']]) {
        barngfUri = it.graph[1]['@id']
    }

    selectBySqlWhere(where) { data ->
        Map work = data.graph[1]['instanceOf']
        List genreForm = work['genreForm']

        String replacementUri = saogfUri

        if (work.containsKey('intendedAudience')) {
            List intendedAudience = asList(work['intendedAudience'])
            if (intendedAudience.any { it['@id'] == 'https://id.kb.se/marc/Juvenile' })
                replacementUri = barngfUri
        }

        DocumentUtil.traverse(genreForm) { value, path ->
            if (path && path.last() == '@id' && value == oldgfUri) {
              // In case the new URI is already linked, don't create a duplicate
              return ['@id':replacementUri] in genreForm ? new DocumentUtil.Remove() : new DocumentUtil.Replace(replacementUri)
            }
        }

        data.scheduleSave()
    }

    selectByIds([gf['oldgf']]) {
        it.scheduleDelete()
    }
}

List asList(o) {
    return (o instanceof List) ? o : [o]
}
