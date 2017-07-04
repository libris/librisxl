package whelk

import groovy.json.JsonBuilder
import groovy.util.logging.Log4j2 as Log

/**
 * Created by Theodor on 2016-11-21.
 * Matches a set of authority records with a bibliographic record and supplies $0 on the fields that is matched
 */
@Log
class AuthBibMatcher {

    static List ignoredAuthFields = ['180', '181', '182', '185', '162']

    //TODO: These rules should be put somewhere else and be made configurable outside of compiled code.
    static Map fieldRules = [
            '100': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['100', '600', '700', '800']],
            '110': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    subFieldsToMatch:  ['a','b'],
                    bibFields        : ['110', '610', '710']],
            '111': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['111', '611', '711']],
            '130': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['130', '630', '730', '830']],
            '148': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['648']],
            '150': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['650'],
                    authFieldsToAdd  : [[field: '040', subfield: 'f', targetField: '2']]],
            '151': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['651']],
            '155': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['655'],
                    authFieldsToAdd  : [[field: '040', subfield: 'f', targetField: '2']]]
    ]

    static List matchCombinationsToIgnore = [
           [authField:'150', pattern:['2','a','2']],
           [authField:'155', pattern:['2','a','2']],
           [authField:'155', pattern:['2','a','']],
           [authField:'155', pattern:['','a','2']],

    ]

    /**
     * Filters the fieldrules for the field names that we will match
     * @return
     */
     static ArrayList getAuthLinkableFields() {
        fieldRules.collectMany { rule ->
            rule.getValue().bibFields.collect { s ->
                [authfield: rule.getKey(), bibField: s]
            }
        }
    }

    /**
     * Main method
     * @param bibRecord The bibliographic record
     * @param authRecords The authoroty records linked to the bib record
     * @param generateStats
     * @return
     */
    static List matchAuthToBib(Map bibRecord, List authRecords) {
        List statsResults = []
        if (bibRecord != null) {
            //Prepare authRecords groups
            def groupedAuthRecords = prepareAuthRecords(authRecords.findAll{it})
            def groupedBibFields = prepareBibRecords(bibRecord, fieldRules)

            groupedAuthRecords.each { authRecordGroup ->
                def bibFieldGroup = groupedBibFields.find { it.key == authRecordGroup.key }
                if (bibFieldGroup?.value) {
                    authRecordGroup.value.each { authRecord ->
                        statsResults.addAll(doMatch(authRecord, bibFieldGroup.value, fieldRules, bibRecord))
                    }
                }
            }
        }
        return statsResults
    }

    static doMatch(Map authRecord, List<Map> bibFieldGroup, fieldRules, bibRecord) {
        List<Map> statsResults = []

        bibFieldGroup.each { Map bibField ->
            Map rule = fieldRules[authRecord.field] as Map
            if (rule) {
                List<Map> bibSubFields = normaliseSubfields(bibField[bibField.keySet()[0]].subfields).findAll {
                    !rule.subFieldsToIgnore.bib.contains(it.keySet()[0])
                }
                def sets = collectSets(bibSubFields.collect(), authRecord)

                addMatchResult(sets)

                handleMultipleLinkedFields(sets,bibRecord, authRecord, bibField)

                statsResults.add(composeStatsResult(sets,authRecord,bibRecord,bibField))
            }
        }
        return statsResults
    }

    static void handleMultipleLinkedFields(sets, bibRecord, authRecord, bibField){
        if (sets.isMatch) {
            Map matchedField = bibRecord.fields.find { it -> it == bibField } as Map
            if (hasSubfield(matchedField,'0')) {
                def linkedAuthIds = getSubfields(matchedField,'0').collect { Map it -> it[it.keySet()[0]] }
                if (!linkedAuthIds.contains(authRecord.id))
                    log.info "Probable double authorityLink for bib record ${authRecord.bibid}: ${linkedAuthIds} and ${authRecord.id}"
            }
            matchedField[matchedField.keySet()[0]].subfields.add(['0': authRecord.id])
        }
    }

   static def collectSets(List<Map> bibSubFields, Map normalizedAuthSubFields){
        def authSet = normalizedAuthSubFields.normalizedSubfields.toSet()
        def bibSet= bibSubFields.toSet()
        return [
                authSet    : authSet,
                bibSet     : bibSet,
                diff       : authSet - bibSet,
                reverseDiff: bibSet - authSet,
                overlap    : bibSet.intersect(authSet)
        ]
    }

    static void addMatchResult(Map sets){
        boolean isMatch = (sets.diff.count { it } == 0 &&
                sets.reverseDiff.count { it } == 0 &&
                sets.overlap.count {it} == sets.bibSet.count { it })
        sets['isMatch'] = isMatch
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
            assert authRecord?.field
            if (!ignoredAuthFields.contains(authRecord.field) &&
                    fieldRules[authRecord.field] != null &&
                    fieldRules[authRecord.field].subFieldsToIgnore != null) {
                authRecord.put("normalizedSubfields",
                        normaliseSubfields(authRecord.subfields)
                                .findAll { it -> !fieldRules[authRecord.field].subFieldsToIgnore.auth.contains(it.keySet()[0]) }
                                .collect { it -> it })
            }
        }

        return authRecords.groupBy { it.field }
                .toSorted {
            -(it.hasProperty('normalizedSubfields') ?
                    it.normalizedSubfields.count { it } : 0)
        }
    }

    static Map composeStatsResult(sets, setSpec, doc, bibField) {
        try {
            boolean hasMisMatchOnA = sets.reverseDiff.find { it -> it.a } != null && sets.diff.find { it -> it.a } != null
            boolean hasOnlyDiff = sets.diff.count { it } > 0 && sets.reverseDiff.count { it } == 0
            boolean hasOnlyReverseDiff = sets.diff.count { it } == 0 && sets.reverseDiff.count { it } > 0
            boolean hasDoubleDiff = sets.diff.count { it } > 0 && sets.reverseDiff.count { it } > 0

            boolean partialMatchOnSubfieldD = getPartialMatchOnSubfieldD(sets.diff, sets.reverseDiff, setSpec.field)

            def result = [
                    diff                  : sets.diff,
                    reverseDiff           : sets.reverseDiff,
                    bibField              : bibField.keySet().first(),
                    authField             : setSpec.field,
                    spec                  : setSpec,
                    errorMessage          : "",
                    overlap               : sets.overlap,
                    numBibFields          : sets.bibSet.count { it },
                    numAuthFields         : sets.authSet.count { it },
                    subfieldsInOverlap    : sets.overlap.collect { it -> it.keySet().first() }.toSorted().join(''),
                    subfieldsInDiff       : sets.diff.collect { it -> it.keySet().first() }.toSorted().join(''),
                    subfieldsInReversediff: sets.reverseDiff.collect { it -> it.keySet().first() }.toSorted().join(''),
                    isMatch               : sets.isMatch,
                    hasMisMatchOnA        : hasMisMatchOnA,
                    hasOnlyDiff           : hasOnlyDiff,
                    hasOnlyReverseDiff    : hasOnlyReverseDiff,
                    hasDoubleDiff         : hasDoubleDiff,
                    bibHas035a            : getHas035a(doc),
                    bibHas240a            : getHas240a(doc),
                    partialD              : partialMatchOnSubfieldD,
                    bibSet                : sets.bibSet,
                    authSet               : sets.authSet,
                    bibId                 : setSpec.bibid,
                    authId                : setSpec.id
            ]
            result.type = getMatchType(result)
            return result
        }
        catch (any) {
            println "Could not generate stats Map"
            println any.message
            println any.stackTrace
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
                if (fieldRules.containsKey(key) && fieldRules[key].authFieldsToAdd != null) {
                    fieldRules[key].authFieldsToAdd.each { add ->
                        def extrafields = getSubfield(map.data, add.field, add.subfield)
                        extrafields.each { extrafield ->
                            map.subfields << [(add.targetField): extrafield]
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
            case {isIgnored(map)}:
                return "ignored"
            case { it.isMatch }:
                return "match"
            case { it.hasMisMatchOnA }:
                return "hasMisMatchOnA"
            case { it.hasOnlyDiff }:
                return "hasOnlyDiff"
            case { it.hasOnlyReverseDiff }:
                return "hasOnlyReverseDiff"
            case { it.hasDoubleDiff }:
                return "hasDoubleDiff"
            default:
                "other"
        }

    }
    static boolean isIgnored(Map map){
        matchCombinationsToIgnore.any { Map combo ->
            if (!map.isMatch && combo.authField == map.authField) {
                String recordCombo = map.subfieldsInDiff + '-' + map.subfieldsInOverlap + '-'+ map.subfieldsInReversediff
                String comboString = combo.pattern.join('-')
                boolean result = recordCombo.equals(comboString)
                return result
            }
            else return false
        }
    }

    static def normaliseSubfields(subfields) {
        subfields.collect {
            it.collect { k, v -> [(k): (v as String).replaceAll(/(^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+\u0024)|(\s)/, "").toLowerCase()] }[0]
        }

    }

    static def getSubfields(Map field, String key) {
        field[field.keySet()[0]].subfields.findAll { it -> it.keySet().first() == key }
    }

    static boolean hasSubfield(Map field, String key){
        return field[field.keySet()[0]].subfields.any { it -> it.keySet().first() == key }
    }

}
