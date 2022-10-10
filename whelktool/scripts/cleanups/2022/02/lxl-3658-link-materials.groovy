PrintWriter linked = getReportWriter('linked.txt')
PrintWriter changedType = getReportWriter('changed-type.txt')

MATERIAL_BASE_URI = 'https://id.kb.se/material/'

LABEL_MAPPINGS =
        [
                'Paper'      : ~/pap[pi]?er/,
                'Parchment'  : ~/pergament/,
                'Plastic'    : ~/plast(ic)?/,
                'Glass'      : ~/glas/,
                'Metal'      : ~/metall?/,
                'Canvas'     : ~/målarduk/,
                'Textile'    : ~/textile?/,
                'Vinyl'      : ~/vinyl/,
                'Plaster'    : ~/gips/,
                'Watercolour': ~/akvarell|vattenfärg/,
                'Gouache'    : ~/gouas?che?/,
                'Charcoal'   : ~/kol/,
                'Ink'        : ~/bläck/,
                'OilPaint'   : ~/oljefärg|olja/,
                'Synthetic'  : ~/syntetiskt material/,
                'Lacquer'    : ~/fernissa/,
                'Chalk'      : ~/krita/,
                'Cardboard'  : ~/papp|kartong/,
                'Pastel'     : ~/torrpastell/
        ]

List modifiedIds = Collections.synchronizedList([])

['baseMaterial', 'appliedMaterial'].each { property ->
    selectByIds(queryIds([('exists-' + property): ['true']]).collect()) { data ->
        def id = data.doc.shortId
        def thing = data.graph[1]
        def modified = false
        def add = [] as Set

        thing[property].removeAll { m ->
            if (m.'@type') {
                def foundLinks = findLinks(m)
                if (foundLinks) {
                    linked.println("$id\t${m}\t${foundLinks}")
                    add += foundLinks
                    return modified = true
                }
                changedType.println("$id\t${m}")
                m['@type'] = 'Material'
                modified = true
            }
            return false
        }

        thing[property] += add.collect { ['@id': MATERIAL_BASE_URI + it] }

        if (modified) {
            modifiedIds << id
            data.scheduleSave()
        }
    }
}

// Clean up duplicates (caused by some marc enums getting normalized into id.kb.se/material uris upon saving)
selectByIds(modifiedIds) {
    def thing = it.graph[1]
    def newBm = thing.baseMaterial?.unique(false)
    def newAm = thing.appliedMaterial?.unique(false)

    if (newBm != thing.baseMaterial) {
        thing.baseMaterial = newBm
        it.scheduleSave()
    }
    if (newAm != thing.appliedMaterial) {
        thing.appliedMaterial = newAm
        it.scheduleSave()
    }
}

List findLinks(Map material) {
    def label = material.label in List ? material.label.join(' & ') : material.label
    def splitLabel = label.split(/ (och|&) |, ?/)
    def mappedParts = splitLabel.findResults { part -> LABEL_MAPPINGS.find { part.trim() ==~ /(?i)${it.value}/ }?.key }

    if (splitLabel.size() == mappedParts.size())
        return mappedParts.unique()

    return []
}


