package whelk.converter.marc

import whelk.component.DocumentNormalizer
import whelk.filter.LanguageLinker
import whelk.util.DocumentUtil
import whelk.util.Romanizer

import static whelk.Document.deepCopy
import static whelk.JsonLd.asList

class RomanizationStep extends MarcFramePostProcStepBase {
    MarcFrameConverter converter
    LanguageLinker langLinker

    Map tLangCodes
    Map langToLangTag
    Map langAliases
    Map byLangToBase
    Map langToTLang

    Map scriptToCode =
            [
                    'https://id.kb.se/i18n/script/Arab': '/(3/r',
                    'https://id.kb.se/i18n/script/Cyrl': '/(N',
                    'https://id.kb.se/i18n/script/Grek': '/(S',
                    'https://id.kb.se/i18n/script/Hani': '/$1',
                    'https://id.kb.se/i18n/script/Hant': '/$1',
//                'https://id.kb.se/i18n/script/Deva': ??
//                'https://id.kb.se/i18n/script/Mong': ??
            ]

    String OG_MARK = '**OG**'
    String HAS_BIB880 = 'marc:hasBib880'
    String BIB880 = 'marc:bib880'
    String PART_LIST = 'marc:partList'
    String FIELDREF = 'marc:fieldref'
    String SUBFIELDS = 'subfields'
    String IND1 = 'ind1'
    String IND2 = 'ind2'

    List FIELDREFS = [FIELDREF, 'marc:bib035-fieldref', 'marc:bib041-fieldref',
                      'marc:bib250-fieldref', 'marc:hold035-fieldref']

    void modify(Map record, Map thing) {
        def work = thing.instanceOf

        // TODO: Do we really want to remove everything? What about "00" fields?
        // https://katalogverk.kb.se/katalogisering/Formathandboken/Lankning/index.html
        def hasBib880 = thing.remove(HAS_BIB880)

        Map bib880ByField = [:]

        asList(hasBib880).eachWithIndex { bib880, i ->
            def linkField = bib880[PART_LIST][0][FIELDREF].split('-')
            def tag = linkField[0]
            def seqNum = linkField[1].take(2)
            if (seqNum != '00') {
                def entry = bib880ByField.computeIfAbsent(tag, s -> [])
                entry.add(['ref': '880-' + seqNum, 'bib880': bib880])
            }
        }

        Map bib880Map = [:]
        Map sameField = [:]

        bib880ByField.each { tag, data ->
            def marcJson = null
            try {
                marcJson = data.collect { bib880ToMarcJson(it.bib880) }
            } catch (Exception e) {
                return
            }
            def marc = [leader: "00887cam a2200277 a 4500", fields: marcJson]
            def converted = converter.runConvert(marc)

            def refs = data.collect {
                bib880Map[it.ref] = converted
                it.ref
            } as Set

            refs.each {
                sameField[it] = refs as Set
            }
        }

        Set handled = []
        def handle880Ref = { ref, path ->
            def converted = bib880Map[ref]
            if (sameField[ref]?.intersect(handled) || converted && mergeAltLanguage(converted.mainEntity, thing, asList(work.language))) {
                handled.add(ref)
                return new DocumentUtil.Remove()
            }
        }

        if (bib880Map) {
            FIELDREFS.each {
                DocumentUtil.findKey(thing, it, handle880Ref)
            }
        }
    }

    void unmodify(Map record, Map thing) {
        def byLangPaths = findByLangPaths(thing)
        def uniqueTLangs = findUniqueTLangs(thing, byLangPaths)

        // TODO: Can multiple transform rules be applied to the same record? What happens then with non-repeatable fields?
        uniqueTLangs.each { tLang ->
            def copy = deepCopy(record)
            def thingCopy = copy.mainEntity
            putOriginalLiteralInNonByLang(thingCopy, byLangPaths, tLang)
            def reverted = converter.runRevert(copy)
            /*
             We expect the order in reverted.fields to mirror the order in the "json data" for repeatable fields
             i.e. if e.g.
             thing.hasTitle[0] = {'@type': 'Title', 'mainTitle': 'Title 1'}
             thing.hasTitle[1] = {'@type': 'Title', 'mainTitle': 'Title 2'}
             then
             reverted.fields[i] = {'245': {'subfields': [{'a': 'Title 1'}]}}
             reverted.fields[i + 1] = {'245': {'subfields': [{'a': 'Title 2'}]}}

             If this can't be achieved we probably need to add some kind of reference to each
             (romanized) object *before* reverting in order to get the fieldrefs right.
             */

            def fieldrefLocationToFieldRef = [:]
            DocumentUtil.findKey(thingCopy, '_revertedBy') { value, path ->
                def fieldMap = reverted.fields.find { it.containsKey(value) }
                def fieldNumber = value
                def field = fieldMap[fieldNumber]

                // TODO: less hacky way to track romanized strings
                def romanizedSubfields = field[SUBFIELDS].findResults {
                    def subfield = it.keySet()[0]
                    // TODO: only romanized subfields or all?
                    if (it[subfield].startsWith(OG_MARK)) {
                        return [(BIB880 + '-' + subfield): it[subfield].replace(OG_MARK, '')]
                    }
                    return null
                }

                if (!romanizedSubfields) {
                    return
                }

                def hasBib880 = thing.computeIfAbsent(HAS_BIB880, s -> [])
                def fieldrefIdx = zeroFill(hasBib880.size() + 1)
                def ref = "$fieldNumber-$fieldrefIdx${scriptToCode[tLangCodes[tLang].fromLangScript] ?: ''}" as String
                def bib880 =
                        [
                                (TYPE)          : 'marc:Bib880',
                                (PART_LIST)     : [[(FIELDREF): ref]] + romanizedSubfields,
                                (BIB880 + '-i1'): field[IND1],
                                (BIB880 + '-i2'): field[IND2]
                        ]

                hasBib880.add(bib880)
                reverted.fields.remove(fieldMap)

                fieldrefLocationToFieldRef[path.collect().dropRight(1)] = "880-$fieldrefIdx" as String

                return
            }

            fieldrefLocationToFieldRef.each { path, ref ->
                def refLocation = DocumentUtil.getAtPath(thing, path)
                def uniqueRefs = asList(refLocation[FIELDREF]).plus(asList(ref)).unique()
                refLocation[FIELDREF] = uniqueRefs.size() == 1 ? uniqueRefs[0] : uniqueRefs
            }
        }

        putRomanizedLiteralInNonByLang(thing, byLangPaths)
    }

    boolean mergeAltLanguage(Map converted, Map thing, List language) {
        def tmpLang = ['language': language]
        langLinker.linkAll(tmpLang)
        // Since the 880s do not specify which language they are in, we assume that they are in the first work language
        def lang = tmpLang.language.findResult { it[ID] } ?: 'https://id.kb.se/language/und'
        return addAltLang(thing, converted, lang)
    }

    boolean addAltLang(thing, converted, lang) {
        if (!langToLangTag[lang] || !langToTLang[lang]) {
            return false
        }
        def nonByLangPaths = []
        DocumentUtil.findKey(converted, langAliases.keySet()) { value, path ->
            nonByLangPaths.add(path.collect())
            return
        }
        nonByLangPaths.each { path ->
            def containingObject = DocumentUtil.getAtPath(thing, path.dropRight(1))
            if (!containingObject) {
                //TODO: No romanized version seems to exists...
                return
            }

            def byLang = [:]
            asList(containingObject).each {
                it.each { k, v ->
                    if (k == path.last()) {
                        def byLangProp = langAliases[k]
                        byLang[byLangProp] =
                                [
                                        (langToLangTag[lang]): DocumentUtil.getAtPath(converted, path),
                                        (langToTLang[lang])  : v
                                ]
                    }
                }
                it.putAll(byLang)
            }
        }

        return true
    }

    def putRomanizedLiteralInNonByLang(Map thing, List<List> byLangPaths) {
        byLangPaths.each {
            def path = it.dropRight(1)
            def containingObject = DocumentUtil.getAtPath(thing, path)
            def nonByLang = [:]

            containingObject.each { k, v ->
                def base = byLangToBase[k]
                if (base) {
                    def romanized = v.findResults { langTag, literal -> langTag in tLangCodes.keySet() ? literal : null }
                    nonByLang[base] = romanized
                }
            }

            nonByLang.each { k, v ->
                if (v.isEmpty()) {
                    containingObject.remove(k)
                } else if (v.size() == 1) {
                    containingObject[k] = v[0]
                } else {
                    containingObject[k] = v
                }
            }
        }
    }

    def putOriginalLiteralInNonByLang(Map thing, List<List> byLangPaths, String tLang) {
        byLangPaths.each {
            def path = it.dropRight(1)
            def containingObject = DocumentUtil.getAtPath(thing, path)
            def nonByLang = [:]
            containingObject.each { k, v ->
                def base = byLangToBase[k]
                if (base) {
                    def romanized = v.find { langTag, literal -> langTag == tLang }
                    def original = v.find { langTag, literal -> langTag == langToLangTag[tLangCodes[tLang].inLanguage] }?.value
                    if (romanized && original) {
                        nonByLang[base] = original instanceof List
                                ? original.collect { OG_MARK + it }
                                : OG_MARK + original
                    }
                }
            }
            containingObject.putAll(nonByLang)
        }
    }

    def findByLangPaths(Map thing) {
        List paths = []

        DocumentUtil.findKey(thing, byLangToBase.keySet()) { value, path ->
            paths.add(path.collect())
            return
        }

        return paths
    }

    Set<String> findUniqueTLangs(Map thing, List<List> byLangPaths) {
        Set tLangs = []

        byLangPaths.each {
            DocumentUtil.getAtPath(thing, it).each { langTag, literal ->
                if (langTag in tLangCodes.keySet()) {
                    tLangs.add(langTag)
                }
            }
        }

        return tLangs
    }

    String zeroFill(int i) {
        i < 10 ? "0$i" : "$i"
    }

    Map bib880ToMarcJson(Map bib880) {
        def parts = bib880[PART_LIST]
        def tag = parts[0][FIELDREF].split('-')[0]
        return [(tag): [
                (IND1)     : bib880["$BIB880-i1"],
                (IND2)     : bib880["$BIB880-i2"],
                (SUBFIELDS): parts[1..-1].collect {
                    def subfields = it.collect { key, value ->
                        [(key.replace('marc:bib880-', '')): value]
                    }
                    return subfields.size() == 1 ? subfields[0] : subfields
                }
        ]]
    }

    void init() {
        this.tLangCodes = getTLangCodes(converter.normalizers)
        this.langToTLang = tLangCodes.collectEntries { k, v -> [v.inLanguage, k] }
        this.langAliases = ld.langContainerAlias
        this.byLangToBase = langAliases.collectEntries { k, v -> [v, k] }
        this.langToLangTag = getLangTags(converter.normalizers)
        this.langLinker = getLangLinker(converter.normalizers)
    }

    static Map<String, Map> getTLangCodes(Collection<DocumentNormalizer> normalizers) {
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

    static Map<String, String> getLangTags(Collection<DocumentNormalizer> normalizers) {
        return normalizers.find { it.normalizer instanceof Romanizer }
                .normalizer
                .langTags
    }

    static LanguageLinker getLangLinker(Collection<DocumentNormalizer> normalizers) {
        return normalizers.find { it.normalizer instanceof LanguageLinker }
                .normalizer
    }
}
