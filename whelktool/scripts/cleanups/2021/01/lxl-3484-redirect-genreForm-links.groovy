import whelk.util.DocumentUtil

List<Map> genreForms = [['oldId': 'xv8cln7g41q4n6h', 'newId': 'gdsw32q030cdkdm'],
                        ['oldId': 'wt7bkm6f11w8wkq', 'newId': 'xv8ckc8g43hck13']]

genreForms.each { gf ->
    String where = "id in (SELECT id FROM lddb__dependencies WHERE dependsonid = '${gf['oldId']}')"
    String oldUri
    String newUri

    selectByIds([gf['oldId']]) {
        oldUri = it.graph[1]['@id']
    }
    selectByIds([gf['newId']]) {
        newUri = it.graph[1]['@id']
    }

    selectBySqlWhere(where) { data ->
        List genreForm = data.graph[1]['instanceOf']['genreForm']

        DocumentUtil.traverse(genreForm) { value, path ->
            if (path && path.last() == '@id' && value == oldUri) {
                // In case the new URI is already linked, don't create a duplicate
                return ['@id':newUri] in genreForm ? new DocumentUtil.Remove() : new DocumentUtil.Replace(newUri)
            }
        }

        data.scheduleSave()
    }

    selectByIds([gf['oldId']]) {
        it.scheduleDelete()
    }

}

