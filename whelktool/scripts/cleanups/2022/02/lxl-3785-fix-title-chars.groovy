/**
 Fix weird characters found in a minority of newspaper titles
 
 See LXL-3785
 */

def where = """
    collection = 'bib'
    AND deleted = 'false'
    AND data#>>'{@graph,1,isIssueOf}' IS NOT NULL
"""

selectBySqlWhere(where) { bib ->
    def (record, thing) = bib.graph

    def titles = getAtPath(thing, ['hasTitle', '*'], [])
    
    titles.each { Map title ->
        String mainTitle = title.mainTitle
        def clean = fixChars(mainTitle)
        if (clean != mainTitle) {
            title.mainTitle = clean
            bib.scheduleSave()
        }
    }

}

// TRANÃS TIDNING -> TRANÅS TIDNING
// SÃFFLETIDNINGEN -> SÄFFLETIDNINGEN
// ÃSTGÃTA CORRESPONDENTEN -> ÖSTGÖTA CORRESPONDENTEN
String fixChars(String s) {
    s.replace([
            'Ã\u0085' : 'Å',
            'Ã\u0084' : 'Ä',
            'Ã\u0096' : 'Ö'
    ])
}

//-----------------------------------------

static Object getAtPath(item, Iterable path, defaultTo = null) {
    if(!item) {
        return defaultTo
    }

    for (int i = 0 ; i < path.size(); i++) {
        def p = path[i]
        if (p == '*' && item instanceof Collection) {
            return item.collect { getAtPath(it, path.drop(i + 1), []) }.flatten()
        }
        else if (item[p] != null) {
            item = item[p]
        } else {
            return defaultTo
        }
    }
    return item
}
