package whelk.tools

import groovy.json.JsonBuilder
import groovy.util.logging.Slf4j as Log

/**
 * Created by Theodor on 2016-11-21.
 * Matches a set of authority records with a bibliographic record and supplies $0 on the fields that is matched
 */
@Log
class SetSpecMatcher {

    static List ignoredAuthFields = ['180', '181', '182', '185', '162', '148']

    //TODO: These rules should be put somewhere else and be made configurable outside of compiled code.
    static Map fieldRules = [
            '100': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['100', '600', '700', '800']],
            '110': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['110', '610', '710']],
            '111': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['111', '611', '711']],
            '130': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['130', '630', '730', '830']],
            '150': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['650']],
            '151': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['651']],
            '155': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['655']]
    ]

    private static ArrayList getAuthLinkableFields() {
        fieldRules.collectMany { rule ->
            rule.getValue().bibFields.collect { s ->
                [authfield: rule.getKey(), bibField: s]
            }
        }
    }

    static boolean hasValidAuthRecords(List authRecords) {
        return (authRecords != null && authRecords.any {
            authRecord -> authRecord != null && !ignoredAuthFields.contains(authRecord.field)
        })
    }

    static matchAuthToBib(Map doc, List allAuthRecords,boolean generateStats = false) {
        List statsResults = []
        if (doc != null && hasValidAuthRecords(allAuthRecords)) {
            List authRecords = allAuthRecords.findAll { it -> !ignoredAuthFields.contains(it.field) }

            //Prepare authRecords groups
            def groupedAuthRecords = prepareAuthRecords(authRecords)
            def groupedBibFields = prepareBibRecords(doc, fieldRules)

            groupedAuthRecords.each { specGroup ->
                def bibFieldGroup = groupedBibFields.find { it.key == specGroup.key }
                if (bibFieldGroup?.value) {
                    specGroup.value.each { spec ->
                        if(generateStats)
                            statsResults.addAll(doMatch(spec, bibFieldGroup.value, fieldRules, doc,true))
                        else
                            doMatch(spec, bibFieldGroup.value, fieldRules, doc)

                    }
                }
            }
        }
        return statsResults
    }

    static doMatch(setSpec, bibFieldGroup, Map fieldRules, Map doc, boolean generateStats = false) {
        List<Map> statsResults = []
        bibFieldGroup.each { field ->
            Map rule = fieldRules[setSpec.field] as Map
            if (rule) {
                def bibSubFields = normaliseSubfields(field[field.keySet()[0]].subfields).findAll {
                    !rule.subFieldsToIgnore.bib.contains(it.keySet()[0])
                }
                Set bibSet = bibSubFields.toSet()
                Set authSet = setSpec.normalizedSubfields.toSet()
                Set diff = authSet - bibSet
                Set reverseDiff = bibSet - authSet
                Set overlap = bibSet.intersect(authSet)
                boolean isMatch = (diff.count { it } == 0 && reverseDiff.count { it } == 0 && overlap.count {
                    it
                } == bibSet.count { it })
                if (isMatch) {
                    Map matchedField = doc.fields.find { it -> it == field } as Map
                    log.trace "Matched! bib:${setSpec.bibid} auth: ${setSpec.id} ${matchedField}"
                    def hasLinkedAuthId = matchedField[matchedField.keySet()[0]].subfields.any { it -> it.keySet().first() == '0' }
                    if (hasLinkedAuthId) {
                        def linkedAuthIds = matchedField[matchedField.keySet()[0]].subfields.findAll { it -> it.keySet().first() == '0' }.collect { Map it -> it[it.keySet()[0]] }
                        if (!linkedAuthIds.contains(setSpec.id))
                            log.info "Bib ${setSpec.bibid} already has a different subfield 0 (${linkedAuthIds}) than matched auth id. Another subfield 0 will be added (${setSpec.id}) ${matchedField}"
                    }
                    matchedField[matchedField.keySet()[0]].subfields.add(['0': setSpec.id])
                    log.trace "Addded ${setSpec.id} as subfield 0 to  bib:${setSpec.bibid} on field ${matchedField}"

                }
                if (generateStats)
                    statsResults.add(composeStatsResult(diff, reverseDiff, overlap, setSpec, bibSet, authSet, isMatch, doc, field))
            }
        }
        return statsResults
    }

    /**
     * Prepares bibRecords for matching. Groups, sorts, and normalizes
     * @param doc
     * @param fieldRules
     * @return
     */
    static def prepareBibRecords(Map doc, fieldRules) {
        def bibFieldGroups =
                doc.fields
                        .findAll { bibField -> authLinkableFields.collect { it -> it.bibField }.contains(bibField.keySet()[0]) }
                        .groupBy { bibField -> authLinkableFields.find { a -> a.bibField == bibField.keySet()[0] }.authfield }

        bibFieldGroups.each { group ->
            group.value.sort { Map field ->
                def subfields = normaliseSubfields(field[field.keySet()[0]].subfields)
                def foundSF = subfields.findAll { it ->
                    !fieldRules[group.key].subFieldsToIgnore.bib.contains(it.keySet()[0])
                }
                return -foundSF.collect { it -> it }.count { it }
            }
        }
    }

    /**
     * Prepares authRecords for matching. Groups, sorts, and normalizes
     * @param authRecords
     * @return
     */
    static def prepareAuthRecords(List authRecords) {

        authRecords.each { Map authRecord ->
            authRecord = composeAuthData(authRecord)
            if (!fieldRules[authRecord.field])
                throw new Exception("No rules for field ${authRecord.field}")
            if (!fieldRules[authRecord.field].subFieldsToIgnore)
                throw new Exception("No subFieldsToIgnore for field ${authRecord.field}")

            authRecord.put("normalizedSubfields",
                    normaliseSubfields(authRecord.subfields)
                            .findAll { it -> !fieldRules[authRecord.field].subFieldsToIgnore.auth.contains(it.keySet()[0]) }
                            .collect { it -> it })
        }
        return authRecords.groupBy { it.field }
                .toSorted {
            -(it.hasProperty('normalizedSubfields') ? it.normalizedSubfields.count { it } : 0)
        }
    }

    static Map composeStatsResult(diff, reverseDiff, overlap, setSpec, bibSet, authSet, isMatch, doc, field) {
        try {
            boolean hasMisMatchOnA = reverseDiff.find { it -> it.a } != null && diff.find { it -> it.a } != null
            boolean hasOnlyDiff = diff.count { it } > 0 && reverseDiff.count { it } == 0
            boolean hasOnlyReverseDiff = diff.count { it } == 0 && reverseDiff.count { it } > 0
            boolean hasDoubleDiff = diff.count { it } > 0 && reverseDiff.count { it } > 0

            boolean partialMatchOnSubfieldD = getPartialMatchOnSubfieldD(diff, reverseDiff, setSpec.field)

            def result = [
                    diff                  : diff,
                    reverseDiff           : reverseDiff,
                    bibField              : field.keySet().first(),
                    spec                  : setSpec,
                    errorMessage          : "",
                    overlap               : overlap,
                    numBibFields          : bibSet.count { it },
                    numAuthFields         : authSet.count { it },
                    subfieldsInOverlap    : overlap.collect { it -> it.keySet().first() }.toSorted().join(''),
                    subfieldsInDiff       : diff.collect { it -> it.keySet().first() }.toSorted().join(''),
                    subfieldsInReversediff: reverseDiff.collect { it -> it.keySet().first() }.toSorted().join(''),
                    isMatch               : isMatch,
                    hasMisMatchOnA        : hasMisMatchOnA,
                    hasOnlyDiff           : hasOnlyDiff,
                    hasOnlyReverseDiff    : hasOnlyReverseDiff,
                    hasDoubleDiff         : hasDoubleDiff,
                    bibHas035a            : getHas035a(doc),
                    bibHas240a            : getHas240a(doc),
                    partialD              : partialMatchOnSubfieldD,
                    bibSet                : bibSet,
                    authSet               : authSet
            ]
            result.type = getMatchType(result)
            return result
        }
        catch (any) {
            println "Could not generate stats Map"
            return null
        }
    }

    /**
     * Finds out if there is a mismatch in 100 $d but if the first four digits are the same (death date is missing)
     * @param diff
     * @param reverseDiff
     * @param specField
     * @return
     */
    static boolean getPartialMatchOnSubfieldD(Set diff, Set reverseDiff, specField) {
        boolean partialD = false
        try {
            String bibD = reverseDiff.find { it.d != null && it?.d.length() > 3 }?.d
            String authorityD = diff.find { it.d != null && it?.d.length() > 3 }?.d
            if (specField == '100' && bibD && authorityD) {
                partialD = (bibD.substring(0, 4) == authorityD.substring(0, 4))
            }
        }
        catch (any) {
            println "error: ${any.message} 100 \$d "
        }
        return partialD
    }

    static Map composeAuthData(LinkedHashMap<String, Object> map) {
        for (Map bibField in map.data.fields) {
            String key = bibField.keySet()[0]
            if (key.startsWith('1')) {
                map.field = key
                map.subfields = bibField[key].subfields
                //Stub for manipulating matching of records. TODO: Make these rules configurable and put them together with similar stuff.
                if (key == '155') {
                    def system = getSubfield(map.data, '040', 'f')
                    if (system) {
                        system.each { s ->
                            map.subfields << ['2': s]
                        }
                    }
                }

                return map
            }
        }
        def builder = new JsonBuilder(map)
        throw new Exception("Unhandled authority record ${builder.toPrettyString()} q")
    }

    static boolean getHas035a(p) {

        try {
            def a = p.fields?.'035'?.subfields?.a
            return a && a.any() && a.first() && a.first().any()
        }
        catch (any) {
            println any.message
            throw any
        }
    }

    private static String[] getSubfield(Map doc, String field, String subfield) {
        def fields = doc.get("fields")
        for (def f : fields) {
            if (f.get(field) != null && f.get(field).subfields.any { it -> it.get('f') })
                return f.get(field).subfields.findAll { it -> it.get('f') }.collect { it.find().value }
        }
        return null
    }


    static boolean getHas240a(p) {

        try {
            def a = p.fields?.'240'?.subfields?.a
            return a && a.any() && a.first() && a.first().any()
        }
        catch (any) {
            println any.message
            throw any
        }
    }

    static String getMatchType(Map map) {
        switch (map) {
            case map.isMatch:
                return "match"
                break
            case map.hasOnlyDiff:
                return "hasOnlyDiff"
                break
            case map.hasOnlyReverseDiff:
                return "hasOnlyReverseDiff"
                break
            case map.hasDoubleDiff:
                return "hasDoubleDiff"
                break
            case map.hasMisMatchOnA:
                return "hasMisMatchOnA"
                break
            default:
                "other"
        }

    }

    static def normaliseSubfields(subfields) {
        subfields.collect {
            it.collect { k, v -> [(k): (v as String).replaceAll(/(^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+\u0024)|(\s)/, "").toLowerCase()] }[0]
        }

    }

    static String getSubfieldValue(Map p, String s) {

        for (subfield in p.subfields) {
            String key = subfield.keySet()[0]
            if (key == s) {
                return subfield[key]
            }
        }
        return ""
    }

}

