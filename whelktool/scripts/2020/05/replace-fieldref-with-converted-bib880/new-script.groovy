import whelk.filter.LanguageLinker
import whelk.util.Romanizer
import whelk.util.DocumentUtil

Util u = new Util(getWhelk())

def where = """
    collection = 'bib'
    AND data#>>'{@graph,1,${u.HAS_BIB880}}' IS NOT NULL
"""

selectBySqlWhere(where) {
    def (record, thing) = it.graph

    def hasBib880 = thing.remove(u.HAS_BIB880)

    def failed = false

    Map bib880ByField = [:]
    asList(hasBib880).each { bib880 ->
        def partList = asList(bib880[u.PART_LIST])
        if (!partList) {
            incrementStats('edge cases', 'missing partlist')
            failed = true
            return
        }
        def linkField = partList[0][u.FIELDREF]
        if (!(linkField =~ /^\d{3}-\d{2}/)) {
            incrementStats('edge cases', 'missing or malformed fieldref')
            failed = true
            return
        }
        def parts = linkField.split('-')
        def tag = parts[0]
        def seqNum = parts[1].take(2)
        if (seqNum != '00') {
            def entry = bib880ByField.computeIfAbsent(tag, s -> [])
            entry.add(['ref': '880-' + seqNum, 'bib880': bib880])
        }
    }

    Map bib880Map = [:]
    Map sameField = [:]

    bib880ByField.each { tag, data ->
        def converted = null
        try {
            def marcJson = data.collect { bib880ToMarcJson(it.bib880, u) }
            def marc = [leader: "00887cam a2200277 a 4500", fields: marcJson]
            converted = u.converter.runConvert(marc)
        } catch (Exception e) {
            incrementStats('edge cases', 'failed conversion')
            failed = true
            return
        }

        def refs = data.collect {
            bib880Map[it.ref] = converted
            it.ref
        } as Set

        refs.each {
            sameField[it] = refs
        }
    }

    Set handled = []
    def handle880Ref = { ref, path ->
        def converted = bib880Map[ref]
        if (sameField[ref]?.intersect(handled) || converted && mergeAltLanguage(converted['@graph'][1], thing, u)) {
            handled.add(ref)
            return new DocumentUtil.Remove()
        }
    }

    if (!failed) {
        u.FIELDREFS.each {
            DocumentUtil.findKey(thing, it, handle880Ref)
        }
        it.scheduleSave()
    }
}

def getWhelk() {
    def whelk = null

    selectByIds(['https://id.kb.se/marc']) {
        whelk = it.whelk
    }

    return whelk
}

boolean mergeAltLanguage(Map converted, Map thing, Util u) {
    // Since the 880s do not specify which language they are in, we assume that they are in the first work language
    def lang = thing.instanceOf.subMap('language').with {
        u.langLinker.linkAll(it)
        asList(it.language).findResult { it[u.ID] } ?: 'https://id.kb.se/language/und'
    }

    return addAltLang(thing, converted, lang, u)
}

boolean addAltLang(Map thing, Map converted, String lang, Util u) {
    if (!u.langToLangTag[lang] || !u.langToTLang[lang]) {
        return false
    }

    def nonByLangPaths = []
    DocumentUtil.findKey(converted, u.langAliases.keySet()) { value, path ->
        nonByLangPaths.add(path.collect())
        return
    }

    nonByLangPaths.each { path ->
        def containingObject = getAtPath(thing, path.dropRight(1))
        if (!containingObject) {
            //TODO: No romanized version seems to exists...
            return
        }

        def byLang = [:]
        asList(containingObject).each {
            it.each { k, v ->
                if (k == path.last()) {
                    def byLangProp = u.langAliases[k]
                    byLang[byLangProp] =
                            [
                                    (u.langToLangTag[lang]): getAtPath(converted, path),
                                    (u.langToTLang[lang])  : v
                            ]
                }
            }
            it.putAll(byLang)
        }
    }

    return true
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

class Util {
    def converter
    def langLinker

    Map tLangCodes
    Map langToTLang
    Map langAliases
    Map langToLangTag

    static final String HAS_BIB880 = 'marc:hasBib880'
    static final String BIB880 = 'marc:bib880'
    static final String PART_LIST = 'marc:partList'
    static final String FIELDREF = 'marc:fieldref'
    static final String SUBFIELDS = 'subfields'
    static final String IND1 = 'ind1'
    static final String IND2 = 'ind2'
    static final String ID = '@id'

    List FIELDREFS = [FIELDREF, 'marc:bib035-fieldref', 'marc:bib041-fieldref',
                      'marc:bib250-fieldref', 'marc:hold035-fieldref']

    Util(whelk) {
        this.converter = whelk.marcFrameConverter
        this.langLinker = getLangLinker(whelk.normalizer.normalizers)
        this.tLangCodes = getTLangCodes(whelk.normalizer.normalizers)
        this.langToTLang = tLangCodes.collectEntries { k, v -> [v.inLanguage, k] }
        this.langAliases = whelk.jsonld.langContainerAlias
        this.langToLangTag = getLangTags(whelk.normalizer.normalizers)
    }

    static Map<String, Map> getTLangCodes(normalizers) {
        return normalizers.find { it.normalizer instanceof Romanizer }
                .normalizer
                .tLangs
                .collectEntries {
                    def code = it[ID].split('/').last()
                    def data = [:]
                    if (it.inLanguage) {
                        data.inLanguage = it.inLanguage[ID]
                    }
                    if (it.fromLangScript) {
                        data.fromLangScript = it.fromLangScript[ID]
                    }
                    [code, data]
                }
    }

    static Map<String, String> getLangTags(normalizers) {
        return normalizers.find { it.normalizer instanceof Romanizer }
                .normalizer
                .langTags
    }

    static LanguageLinker getLangLinker(normalizers) {
        return normalizers.find { it.normalizer instanceof LanguageLinker }
                .normalizer
    }
}