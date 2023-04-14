import whelk.util.Unicode
import whelk.util.DocumentUtil

Util u = new Util(getWhelk())

romanized = getReportWriter("romanized.tsv")
missingRomanized = getReportWriter("missing-romanized.tsv")
unsafePath = getReportWriter("unsafe-path.tsv")
lost = getReportWriter("lost-values.tsv")
looksLikeLatin = getReportWriter("looks-like-latin.tsv")

def where = """
    collection = 'bib'
    AND data#>>'{@graph,1,${u.HAS_BIB880}}' IS NOT NULL
"""

selectBySqlWhere(where) { bib ->
    def (record, thing) = bib.graph
    def id = bib.doc.shortId

    def romanizableLangs = thing.instanceOf.subMap('language').with {
        u.langLinker.linkAll(it)
        asList(it.language).findResults { it[u.ID] in u.romanizableLangs ? it[u.ID] : null }
    }

    def lang = romanizableLangs.size() == 1 ? romanizableLangs[0] : 'https://id.kb.se/language/und'

    if (romanizableLangs.isEmpty()) {
        incrementStats('No romanizable language', asList(thing.instanceOf.language))
    }
    if (romanizableLangs.size() > 2) {
        incrementStats('Multiple romanizable langs', romanizableLangs)
        return
    }

    def langTag = u.langIdToLangTag[lang]
    if (!langTag) {
        incrementStats('Missing langTag for language', lang)
        return
    }
    def tLangTag = "$langTag${u.MATCH_T_TAG}$langTag"

    def hasBib880 = asList(thing.remove(u.HAS_BIB880))

    Map seqNumToBib880Data = collectValidBib880sBySeqNum(hasBib880, u)

    DocumentUtil.findKey(thing, u.FIELDREFS) { ref, path ->
        if (ref instanceof List) {
            if (ref.size() > 1) {
                incrementStats('multiple fieldrefs', 'multiple fieldrefs')
                return
            } else {
                ref = ref[0]
            }
        }

        def seqNum = ref.takeRight(2)
        def bib880data = seqNumToBib880Data[seqNum]
        if (!bib880data) {
            return
        }
        def nonByLangPaths = collectNonByLangPaths(bib880data.converted, u.langAliases.keySet(), id)

        for (List p : nonByLangPaths) {
            def pCopy = p.collect()
            def refPath = path.dropRight(1)

            // Since we convert the bib880s one by one, array indexes in the path will always be 0.
            // However in the description (thing) where we want to insert the bib880 values, there may be more than one object in some arrays.
            // So we check the index of the object containing the fieldref to get the path right.
            // If there is no fieldref in the path it's still ok if no arrays are of of size > 1.
            def isSafePath = adjustArrayIndices(p, refPath) || !existRepeated(thing, p)
            if (!isSafePath) {
                unsafePath.println([id, bib880data.ref, p.join('.'), refPath.join('.')].join('\t'))
                incrementStats('Unsafe path: no fieldref in path and multiple elements in array', p.join('.'))
                return
            }

            def original = getAtPath(bib880data.converted, pCopy)
            if (looksLikeScript(original.toString(), 'Latn')) {
                looksLikeLatin.println([id, bib880data.ref, p.join('.'), original].join('\t'))
                return
            }

            def parentObject = getAtPath(thing, p.dropRight(1))
            if (!parentObject) {
                missingRomanized.println([id, bib880data.ref, p.join('.'), original].join('\t'))
                incrementStats('No romanized value at path', p.join('.'))
                return
            }

            asList(parentObject).each {
                def k = p.last()
                if (it[k]) {
                    def byLangProp = u.langAliases[k]
                    romanized.println([id, tLangTag, p.join('.'), bib880data.ref, original, it[k]].join('\t'))
                    it[byLangProp] =
                            [
                                    (langTag) : original,
                                    (tLangTag): it[k]
                            ]
                    it.remove(k)
                }
            }
        }

        bib880data['handled'] = true
        return new DocumentUtil.Remove()
    }

    def indexesOfHandled = seqNumToBib880Data.findResults { seqNum, data -> data.handled ? data.idxOfOriginal : null }
    if (!indexesOfHandled.isEmpty()) {
        if (indexesOfHandled.size() == hasBib880.size()) {
            incrementStats('Handled records', 'completely handled')
        } else {
            incrementStats('Handled records', 'partly handled')
        }
        indexesOfHandled.sort().reverseEach {
            hasBib880.removeAt(it)
        }
        if (!hasBib880.isEmpty()) {
            thing[u.HAS_BIB880] = hasBib880
        }
        incrementStats('Romanized langs', tLangTag)
        bib.scheduleSave()
    } else {
        incrementStats('Handled records', 'unhandled')
    }
}

Map collectValidBib880sBySeqNum(List hasBib880, Util u) {
    Map seqNumToBib880Data = [:]
    Set duplicateSeqNums = []

    hasBib880.eachWithIndex { bib880, i ->
        def ref = getFieldRef(bib880, u)
        if (ref.isEmpty()) {
            return
        }

        def converted = tryConvert(bib880, u)
        if (converted.isEmpty()) {
            incrementStats('Invalid Bib880', 'failed conversion')
            return
        }

        def seqNum = ref.get().takeRight(2)

        if (seqNumToBib880Data.containsKey(seqNum)) {
            duplicateSeqNums.add(seqNum)
            return
        }

        seqNumToBib880Data[seqNum] = [
                'ref'          : ref.get(),
                'converted'    : converted.get()['@graph'][1],
                'idxOfOriginal': i
        ]
    }

    duplicateSeqNums.each {
        incrementStats('duplicate sequence number', it)
        seqNumToBib880Data.remove(it)
    }

    return seqNumToBib880Data
}

def getWhelk() {
    def whelk = null

    selectByIds(['https://id.kb.se/marc']) {
        whelk = it.whelk
    }

    return whelk
}

Optional<String> getFieldRef(Map bib880, Util u) {
    def partList = asList(bib880[u.PART_LIST])
    if (!partList) {
        incrementStats('Invalid Bib880', 'missing partlist')
        return Optional.empty()
    }
    def fieldref = partList[0][u.FIELDREF]
    if (!fieldref) {
        incrementStats('Invalid Bib880', 'missing fieldref')
        return Optional.empty()
    }
    if (!(fieldref =~ /^\d{3}-\d{2}/)) {
        incrementStats('Invalid Bib880', 'malformed fieldref')
        return Optional.empty()
    }
    return Optional.of(fieldref.take(6))
}

Optional<Map> tryConvert(Map bib880, Util u) {
    def converted = null

    try {
        def marcJson = bib880ToMarcJson(bib880, u)
        def marc = [leader: "00887cam a2200277 a 4500", fields: [marcJson]]
        converted = u.converter.runConvert(marc)
    } catch (Exception e) {
        return Optional.empty()
    }

    return Optional.of(converted)
}

Map bib880ToMarcJson(Map bib880, Util u) {
    def parts = asList(bib880[u.PART_LIST])
    def tag = parts[0][u.FIELDREF].split('-')[0]
    return [(tag): [
            (u.IND1)     : bib880["$u.BIB880-i1"],
            (u.IND2)     : bib880["$u.BIB880-i2"],
            (u.SUBFIELDS): parts[1..-1].collect {
                def subfields = it.collect { key, value ->
                    [(key.replace('marc:bib880-', '')): value]
                }
                return subfields.size() == 1 ? subfields[0] : subfields
            }
    ]]
}

List collectNonByLangPaths(Map thing, Set byLangBaseProps, String id) {
    def nonByLangPaths = []

    DocumentUtil.findKey(thing, byLangBaseProps) { value, path ->
        def stripped = value.toString().replaceAll(~/\p{IsCommon}|\p{IsInherited}|\p{IsUnknown}/, '')

        if (stripped.isEmpty() || stripped.every { looksLikeScript(it, 'Latn') }) {
            lost.println([id, path.join('.'), value].join('\t'))
        } else {
            nonByLangPaths.add(path.collect())
        }

        return
    }

    return nonByLangPaths
}

// Get array index right when the referenced object is contained in an array of size > 1
// For example:
// [publication, 0, place, label] -> [publication, 1, place, label]
// [seriesMembership, 0, inSeries, instanceOf, hasTitle, 0, mainTitle] -> [seriesMembership, 0, inSeries, instanceOf, 1, hasTitle, 0, mainTitle]
// Returns false if refPath is not a subpath of the adjusted path
boolean adjustArrayIndices(List path, List refPath) {
    def copy = path.collect()

    if (path.any { it instanceof Integer }) {
        for (int i : 0..<refPath.size()) {
            if (path[i] instanceof String && refPath[i] instanceof String) {
                if (path[i] != refPath[i]) {
                    path.clear()
                    path.addAll(copy)
                    return false
                }
            } else if (path[i] instanceof Integer && refPath[i] instanceof Integer) {
                if (path[i] != refPath[i]) {
                    path[i] = refPath[i]
                }
            } else if (path[i] instanceof String && refPath[i] instanceof Integer) {
                path.add(i, refPath[i])
            } else if (path[i] instanceof Integer && refPath[i] instanceof String) {
                path.removeAt(i)
            }
        }
    }

    return true
}

// Check if any array in the path is of size > 1
boolean existRepeated(Map thing, List path) {
    def copy = path.collect()

    def existRepeated = path.findIndexValues { it instanceof Integer }
            *.toInteger()
            .any {
                def obj = getAtPath(thing, path.take(it))
                if (obj instanceof Map) {
                    // Not an array at subpath location, adjust accordingly (bättre förklaring)
                    path.removeAt(it)
                }
                return obj instanceof List && obj.size() > 1
            }

    if (existRepeated) {
        path.clear()
        path.addAll(copy)
    }

    return existRepeated
}

boolean looksLikeScript(String s, String scriptCode) {
    Unicode.guessIso15924ScriptCode(s).map { it == scriptCode }.orElse(false)
}

class Util {
    def converter
    def langLinker

    Map langAliases
    Map byLangToBase
    Map langIdToLangTag

    Set romanizableLangs

    static final String TARGET_SCRIPT = 'Latn'
    static final String MATCH_T_TAG = "-${TARGET_SCRIPT}-t-"

    static final String HAS_BIB880 = 'marc:hasBib880'
    static final String BIB880 = 'marc:bib880'
    static final String PART_LIST = 'marc:partList'
    static final String FIELDREF = 'marc:fieldref'
    static final String SUBFIELDS = 'subfields'
    static final String IND1 = 'ind1'
    static final String IND2 = 'ind2'
    static final String ID = '@id'

    List FIELDREFS = [FIELDREF, 'marc:bib250-fieldref']

    Util(whelk) {
        this.langAliases = whelk.jsonld.langContainerAlias
        this.byLangToBase = langAliases.collectEntries { k, v -> [v, k] }
        this.langIdToLangTag = whelk.storage.loadAll("https://id.kb.se/dataset/languages")
                .collect { it.data['@graph'][1] }
                .findAll { it.langTag }
                .collectEntries { [it['@id'], it.langTag] }
        this.converter = whelk.marcFrameConverter
        this.converter.conversion.doPostProcessing = false
        this.langLinker = whelk.languageResources.languageLinker
        this.romanizableLangs = whelk.storage.loadAll("https://id.kb.se/dataset/i18n/tlangs").findResults {
            it.data['@graph'][1].inLanguage?.'@id'
        }
    }
}