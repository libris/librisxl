package whelk.converter.marc

import groovy.transform.MapConstructor
import groovy.util.logging.Log4j2 as Log
import whelk.util.DocumentUtil
import whelk.util.Unicode

import static whelk.Document.deepCopy
import static whelk.JsonLd.asList

@Log
class RomanizationStep extends MarcFramePostProcStepBase {
    private static final String TARGET_SCRIPT = 'Latn'
    private static final String MATCH_T_TAG = "-${TARGET_SCRIPT}-t-"

    boolean requiresResources = true

    MarcFrameConverter converter
    
    Map langAliases
    Map byLangToBase

    Map langIdToLangTag
    Set romanizableLangs

    // Note: MARC standard allows ISO 15924 in $6 but Libris practice doesn't
    private static final Map MARC_SCRIPT_CODES =
            [
                    'Arab': '/(3/r',
                    'Cyrl': '/(N',
                    'Cyrs': '/(N',
                    'Grek': '/(S',
                    'Hang': '/$1',
                    'Hani': '/$1',
                    'Hans': '/$1',
                    'Hant': '/$1',
                    'Hebr': '/(2/r'
            ]

    String OG_MARK = '**OG**'

    String HAS_BIB880 = 'marc:hasBib880'
    String BIB880 = 'marc:bib880'
    String PART_LIST = 'marc:partList'
    String FIELDREF = 'marc:fieldref'
    String SUBFIELDS = 'subfields'
    String IND1 = 'ind1'
    String IND2 = 'ind2'
    String BIB250_REF = 'marc:bib250-fieldref'

    List FIELD_REFS = [FIELDREF, BIB250_REF]

    void modify(Map record, Map thing) {
        def bib880 = thing[HAS_BIB880]
        if (!bib880) {
            return
        }
        try {
            _modify(record, thing)
        } catch (Exception e) {
            if (bib880) {
                thing[HAS_BIB880] = bib880
            }
            log.error("Failed to convert 880 for record ${record[ID]}: $e", e)
        }
    }

    void _modify(Map record, Map thing) {
        if (!resourceCache?.languageResources) {
            return
        }

        def workLang = thing.instanceOf.subMap('language')
        resourceCache.languageResources.languageLinker.linkAll(workLang)
        def romanizable = asList(workLang.language).findResults { it[ID] in romanizableLangs ? it[ID] : null }

        if (romanizable.size() > 1) {
            return
        }

        def lang = romanizable.size() == 1 ? romanizable[0] : 'https://id.kb.se/language/und'
        def langTag = langIdToLangTag[lang]
        if (!langTag) {
            return
        }
        def tLangTag = "$langTag${MATCH_T_TAG}$langTag" as String

        def hasBib880 = asList(thing.remove(HAS_BIB880))

        Map seqNumToBib880Data = collectValidBib880sBySeqNum(hasBib880)

        DocumentUtil.findKey(thing, FIELD_REFS) { ref, path ->
            if (ref instanceof List) {
                if (ref.size() > 1) {
                    return
                } else {
                    ref = ref[0]
                }
            }

            def refPath = path.dropRight(1)
            def seqNum = ref.takeRight(2)

            def bib880data = seqNumToBib880Data[seqNum]
            if (!bib880data) {
                return
            }

            def byLangBasePaths = collectByLangBasePaths(bib880data.converted)

            for (List p : byLangBasePaths) {
                def pCopy = p.collect()

                // Since we convert the bib880s one by one, array indexes in the path will always be 0.
                // However in the description (thing) where we want to insert the bib880 values, there may be more than one object in some arrays.
                // So we check the index of the object containing the fieldref to get the path right.
                // If there is no fieldref in the path it's still ok if no arrays are of size > 1.
                def safePath = adjustArrayIndexes(p, refPath)
                if (!safePath) {
                    if (existRepeated(thing, p)) {
                        return
                    } else {
                        p = removeIndexFromPathIfNotArray(thing, p)
                    }
                }

                def original = DocumentUtil.getAtPath(bib880data.converted, pCopy)
                if (looksLikeLatin(original.toString())) {
                    return
                }

                def parentObject = DocumentUtil.getAtPath(thing, p.dropRight(1))
                if (!parentObject) {
                    return
                }

                asList(parentObject).each {
                    def k = p.last()
                    if (it[k]) {
                        def byLangProp = langAliases[k]
                        it[byLangProp] =
                                [
                                        (langTag) : original,
                                        (tLangTag): it.remove(k)
                                ]
                    }
                }
            }

            bib880data['handled'] = true
            return new DocumentUtil.Remove()
        }

        def indexesOfHandled = seqNumToBib880Data.findResults { seqNum, data -> data.handled ? data.idxOfOriginal : null }
        if (!indexesOfHandled.isEmpty()) {
            indexesOfHandled.sort().reverseEach {
                hasBib880.removeAt(it)
            }
        }
        if (!hasBib880.isEmpty()) {
            thing[HAS_BIB880] = hasBib880
        }
    }

    void unmodify(Map record, Map thing) {
        try {
            _unmodify(record, thing)
        } catch (Exception e) {
            log.error("Failed to convert 880 for record ${record[ID]}: $e", e)
        }
    }

    void _unmodify(Map record, Map thing) {
        def byLangPaths = findByLangPaths(thing)
        def uniqueTLangs = findUniqueTLangs(thing, byLangPaths)

        unmodifyTLangs(thing, uniqueTLangs, byLangPaths, record)

        byLangPaths.each { putRomanizedLiteralInNonByLang(thing, it as List) }
    }

    private def unmodifyTLangs(def thing, def tLangs, def byLangPaths, def record) {
        def bib880ToRef = []

        tLangs.each { tLang ->
            def copy = deepCopy(record)
            def thingCopy = copy.mainEntity

            byLangPaths.each { putOriginalLiteralInNonByLang(thingCopy, it as List, tLang) }

            prepareForRevert(thingCopy)

            def reverted = converter.runRevert(copy)
            def romanizedFieldsByTmpRef = findRomanizedFields(reverted)

            DocumentUtil.findKey(thingCopy, FIELD_REFS) { value, path ->
                def romanizedField = romanizedFieldsByTmpRef[value]
                if (romanizedField) {
                    def fieldNumber = romanizedField.keySet()[0]
                    def field = romanizedField[fieldNumber]

                    def ref = new Ref(
                            toField: fieldNumber,
                            path: path.dropRight(1),
                            scriptCode: marcScript(tLang)
                    )

                    def bib880 =
                            [
                                    (TYPE)          : 'marc:Bib880',
                                    (PART_LIST)     : [[(FIELDREF): fieldNumber]] + field[SUBFIELDS],
                                    (BIB880 + '-i1'): field[IND1],
                                    (BIB880 + '-i2'): field[IND2]
                            ]

                    bib880ToRef.add([bib880, ref])
                }
                return new DocumentUtil.Remove()
            }
        }

        if (bib880ToRef) {
            def sorted = bib880ToRef.sort { it[1].toField }
            sorted.eachWithIndex { entry, i ->
                def (bib880, ref) = entry
                bib880[PART_LIST][0][FIELDREF] = ref.from880(i + 1)
                def t = DocumentUtil.getAtPath(thing, ref.path)
                t[ref.propertyName()] = (asList(t[ref.propertyName()]) << ref.to880(i + 1)).unique()
            }

            thing[HAS_BIB880] = sorted.collect { it[0] }
        }
    }

    private String marcScript(String tLang) {
        MARC_SCRIPT_CODES.findResult { tLang.contains(it.key) ? it.value : null } ?: ''
    }

    private static String stripMark(String s, String mark) {
        // Multiple properties can become one MARC subfield. So marks can also occur inside strings.
        s.startsWith(mark)
                ? s.replace(mark, '')
                : s
    }

    @MapConstructor
    private class Ref {
        String toField
        String scriptCode
        List path

        String from880(int occurenceNumber) {
            "$toField-${String.format("%02d", occurenceNumber)}${scriptCode ?: ''}"
        }

        String to880(int occurenceNumber) {
            "880-${String.format("%02d", occurenceNumber)}"
        }

        String propertyName() {
            return toField == '250' ? BIB250_REF : FIELDREF
        }
    }

    Map collectValidBib880sBySeqNum(List hasBib880) {
        Map seqNumToBib880Data = [:]
        Set duplicateSeqNums = []

        hasBib880.eachWithIndex { bib880, i ->
            def ref = getFieldRef(bib880)
            if (ref.isEmpty()) {
                return
            }

            def converted = tryConvert(bib880)
            if (converted.isEmpty()) {
                return
            }

            def seqNum = ref.get().takeRight(2)

            if (seqNumToBib880Data.containsKey(seqNum)) {
                duplicateSeqNums.add(seqNum)
                return
            }

            seqNumToBib880Data[seqNum] = [
                    'ref'          : ref.get(),
                    'converted'    : converter.conversion.flatLinkedForm ? converted.get()['@graph'][1] : converted.get()['mainEntity'],
                    'idxOfOriginal': i
            ]
        }

        duplicateSeqNums.each {
            seqNumToBib880Data.remove(it)
        }

        return seqNumToBib880Data
    }

    Optional<String> getFieldRef(Map bib880) {
        def partList = asList(bib880[PART_LIST])
        if (!partList) {
            return Optional.empty()
        }
        def fieldref = partList[0][FIELDREF]
        if (!(fieldref =~ /^\d{3}-\d{2}/)) {
            return Optional.empty()
        }
        return Optional.of(fieldref.take(6))
    }

    Optional<Map> tryConvert(Map bib880) {
        def converted = null

        try {
            def marcJson = bib880ToMarcJson(bib880)
            def marc = [leader: "00887cam a2200277 a 4500", fields: [marcJson]]
            converted = converter.runConvert(marc)
        } catch (Exception e) {
            return Optional.empty()
        }

        return Optional.of(converted)
    }

    Map bib880ToMarcJson(Map bib880) {
        def parts = asList(bib880[PART_LIST])
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

    List collectByLangBasePaths(Map thing) {
        def byLangBasePaths = []

        DocumentUtil.findKey(thing, langAliases.keySet()) { value, path ->
            if (!allLatin(value)) {
                byLangBasePaths.add(path.collect())
            }
            return
        }

        return byLangBasePaths
    }

    boolean allLatin(s) {
        def stripped = s.toString().replaceAll(~/\p{IsCommon}|\p{IsInherited}|\p{IsUnknown}/, '')
        return !stripped || stripped.every { looksLikeLatin(it) }
    }

    boolean looksLikeLatin(String s) {
        Unicode.guessIso15924ScriptCode(s).map { it == 'Latn' }.orElse(false)
    }

    // Get array index right when the referenced object is contained in an array of size > 1
    // For example:
    // [publication, 0, place, label] -> [publication, 1, place, label]
    // [seriesMembership, 0, inSeries, instanceOf, hasTitle, 0, mainTitle] -> [seriesMembership, 0, inSeries, instanceOf, 1, hasTitle, 0, mainTitle]
    // Returns false if refPath is not a subpath of the adjusted path
    boolean adjustArrayIndexes(List path, List refPath) {
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
        def nextArrayPath = path.takeWhile { it instanceof String }
        if (nextArrayPath.isEmpty()) {
            return false
        }
        def obj = asList(DocumentUtil.getAtPath(thing, nextArrayPath))
        if (obj.size() > 1) {
            return true
        }
        if (obj[0] instanceof Map) {
            existRepeated(obj[0], path.drop(nextArrayPath.size() + 1))
        } else {
            return false
        }
    }

    // E.g. ['hasDimensions', 0, 'label'] -> ['hasDimensions', 'label'] if thing.hasDimensions is a Map rather than List.
    List removeIndexFromPathIfNotArray(Map thing, List path) {
        def nextArrayPath = path.takeWhile { it instanceof String }
        if (nextArrayPath.isEmpty()) {
            return path
        }
        def obj = DocumentUtil.getAtPath(thing, nextArrayPath)
        if (obj instanceof Map) {
            return nextArrayPath + removeIndexFromPathIfNotArray(obj, path.drop(nextArrayPath.size() + 1))
        }
        if (obj?.getAt(0) instanceof Map) {
            return nextArrayPath + removeIndexFromPathIfNotArray(obj[0], path.drop(nextArrayPath.size()))
        } else {
            return path
        }
    }

    Map findRomanizedFields(Map reverted) {
        Map byTmpRef = [:]

        reverted.fields.each {
            def fieldNumber = it.keySet()[0]
            def field = it[fieldNumber]
            if (field instanceof Map) {
                def sf6 = field[SUBFIELDS]?.find { it.containsKey('6') }
                if (sf6 && field[SUBFIELDS].any { Map sf -> sf.values().any { it.startsWith(OG_MARK) } }) {
                    field[SUBFIELDS] = (field[SUBFIELDS] - sf6).collect {
                        def subfield = it.keySet()[0]
                        [(BIB880 + '-' + subfield): stripMark(it[subfield], OG_MARK)]
                    }
                    def tmpRef = sf6['6'].replaceAll(/[^0-9]/, "")
                    byTmpRef[tmpRef] = it
                }
            }
        }

        return byTmpRef
    }

    def putLiteralInNonByLang(Map thing, List byLangPath, Closure handler) {
        def key = byLangPath.last()
        def path = byLangPath.dropRight(1)
        Map parent = DocumentUtil.getAtPath(thing, path)

        def base = byLangToBase[key]
        if (base && parent[key] && !parent[base]) {
            handler(parent, key, base)
        }
        parent.remove(key)
    }

    def putRomanizedLiteralInNonByLang(Map thing, List byLangPath) {
        putLiteralInNonByLang(thing, byLangPath) { Map parent, String key, String base ->
            def langContainer = parent[key] as Map
            if (langContainer.size() == 1) {
                parent[base] = langContainer.values().first()
            } else {
                pickRomanization(langContainer).values()
                        .with(RomanizationStep::unpackSingle)
                        ?.with { parent[base] = it }
            }
        }
    }

    static Map pickRomanization(Map langContainer) {
        // For now we just take the first tag in alphabetical order
        // works for picking e.g. yi-Latn-t-yi-Hebr-m0-alaloc over yi-Latn-t-yi-Hebr-x0-yivo
        langContainer.findAll { String langTag, literal -> langTag.contains(MATCH_T_TAG) }.sort()?.take(1) ?: [:]
    }

    static def unpackSingle(Collection l) {
        return l.size() == 1 ? l[0] : l
    }

    def prepareForRevert(Map thing) {
        def tmpRef = 1
        DocumentUtil.traverse(thing) { value, path ->
            // marc:nonfilingChars is only valid for romanized string
            if (path && path.last() == 'marc:nonfilingChars') {
                return new DocumentUtil.Remove()
            }
            if (value instanceof Map) {
                value[FIELDREF] = tmpRef.toString()
                tmpRef += 1
            }
            return DocumentUtil.NOP
        }
        if (thing['editionStatement']) {
            thing[BIB250_REF] = tmpRef.toString()
        }
    }

    def putOriginalLiteralInNonByLang(Map thing, List byLangPath, String tLang) {
        putLiteralInNonByLang(thing, byLangPath) { Map parent, String key, String base ->
            def romanized = parent[key].find { langTag, literal -> langTag == tLang }
            def original = parent[key].find { langTag, literal -> tLang.contains("${MATCH_T_TAG}${langTag}") }?.value
            if (romanized && original) {
                parent[base] = original instanceof List
                        ? original.collect { OG_MARK + it }
                        : OG_MARK + original
            }
        }
    }

    def findByLangPaths(Map thing) {
        List paths = []

        DocumentUtil.findKey(thing, byLangToBase.keySet()) { value, path ->
            paths.add(path.collect())
            return
        }

        paths = paths.findAll { DocumentUtil.getAtPath(thing, it).keySet().any { it =~ '-t-' } }

        return paths
    }

    Set<String> findUniqueTLangs(Map thing, List<List> byLangPaths) {
        Set tLangs = []

        byLangPaths.each {
            Map<String, ?> langContainer = DocumentUtil.getAtPath(thing, it)
            pickRomanization(langContainer).with { tLangs.addAll(it.keySet()) }
        }

        return tLangs
    }

    void init() {
        if (ld) {
            this.langAliases = ld.langContainerAlias
            this.byLangToBase = langAliases.collectEntries { k, v -> [v, k] }
        }

        if (!resourceCache?.languageResources) {
            return
        }

        var languageResources = resourceCache.languageResources

        this.langIdToLangTag = languageResources.languages
                .findAll { k, v -> v.langTag }.collectEntries { k, v -> [k, v.langTag] }
        this.romanizableLangs = languageResources.transformedLanguageForms
                .findResults { it.value.inLanguage?.get(ID) }
    }
}
