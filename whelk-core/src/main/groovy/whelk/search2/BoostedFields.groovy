package whelk.search2

import groovy.transform.CompileStatic
import groovy.transform.TypeCheckingMode
import whelk.search.ESQueryLensBoost
import whelk.Whelk
import whelk.JsonLd
import whelk.util.DocumentUtil

/*
rest/api/SearchUtils2:

private Map<String, Object> getEsQueryDsl(QueryTree queryTree, QueryParams queryParams, AppParams.StatsRepr statsRepr)

	queryDsl.put("query", queryTree.toEs(queryUtil, disambiguate));

Map getESQuery(Map<String, String[]> ogQueryParameters, String suggest = null, String spell = null) {

*/


//private Map<String, List<String>> boostFieldsByType = [:]
//private ESQueryLensBoost lensBoost

@CompileStatic
class BoostedFields {

private Whelk whelk
private JsonLd jsonld
private Set keywordFields
private Set dateFields
private Set<String> nestedFields
private Set<String> nestedNotInParentFields
private Set<String> numericExtractorFields

BoostedFields(Whelk whelk) {
	this.whelk = whelk
	this.jsonld = whelk.jsonld
	initFieldMappings(this.whelk)
}

void initFieldMappings(Whelk whelk) {
        if (whelk.elastic) {
            Map mappings = whelk.elastic.getMappings()
            this.keywordFields =  getKeywordFields(mappings)
            this.dateFields = getFieldsOfType('date', mappings)
            this.nestedFields = getFieldsOfType('nested', mappings)
            this.nestedNotInParentFields = nestedFields - getFieldsWithSetting('include_in_parent', true, mappings)
            this.numericExtractorFields = getFieldsWithAnalyzer('numeric_extractor', mappings)

/*
            if (DocumentUtil.getAtPath(mappings, ['properties', '_sortKeyByLang', 'properties', 'sv', 'fields', 'trigram'], null)) {
                ENABLE_SPELL_CHECK = true
            }
            log.info("ENABLE_SPELL_CHECK = ${ENABLE_SPELL_CHECK}")
*/
        } else {
            this.keywordFields = Collections.emptySet()
            this.dateFields = Collections.emptySet()
            this.nestedFields = Collections.emptySet()
        }
    }

Set getKeywordFields(Map mappings) {
        Set keywordFields = [] as Set
        if (mappings) {
            keywordFields = getKeywordFieldsFromProperties(mappings['properties'] as Map)
        }

        return keywordFields
    }

private Set getKeywordFieldsFromProperties(Map properties, String parentName = '') {
        Set result = [] as Set
        properties.each { fieldName, fieldSettings ->
            result += getKeywordFieldsFromProperty(fieldName as String,
                        fieldSettings as Map, parentName)
        }

        return result
    }

private Set getKeywordFieldsFromProperty(String fieldName, Map fieldSettings, String parentName) {
        Set result = [] as Set
        String currentField
        if (parentName == '') {
            currentField = fieldName
        } else {
            currentField = "${parentName}.${fieldName}"
        }
        Map fields = (Map) fieldSettings.get('fields')
        if (fields && fields.get('keyword')) {
            result.add(currentField)
        }
        Map properties = (Map) fieldSettings.get('properties')
        if (properties) {
            result += getKeywordFieldsFromProperties(properties, currentField)
        }
        return result
    }

static Set getFieldsWithSetting(String setting, value, Map mappings) {
        Set fields = [] as Set
        DocumentUtil.findKey(mappings['properties'], setting) { v, path ->
            if (v == value) {
                fields.add(path.dropRight(1).findAll{ it != 'properties'}.join('.'))
            }
            DocumentUtil.NOP
        }
        return fields
    }

static Set getFieldsWithAnalyzer(String analyzer, Map mappings) {
        getFieldsWithSetting('analyzer', analyzer, mappings)
    }

static Set getFieldsOfType(String type, Map mappings) {
        getFieldsWithSetting('type', type, mappings)
    }

@CompileStatic(TypeCheckingMode.SKIP)
List<String> boostedFields(Map<String, String[]> ogQueryParameters, ESQueryLensBoost lensBoost) {

        Map<String, String[]> queryParameters = new HashMap<>(ogQueryParameters)

	String[] originalTypeParam = queryParameters.get('@type')
        if (originalTypeParam != null) {
            queryParameters.put('@type', expandTypeParam(originalTypeParam, whelk.jsonld))
        }

	String[] boostParam = queryParameters.get('_boost')

        String boostMode = boostParam ? boostParam[0] : null
        List<String> boostedFields = getBoostFields(originalTypeParam, boostMode, lensBoost)

	return boostedFields
}

List<String> getBoostFields(String[] types, String boostMode, ESQueryLensBoost lensBoost) {
        if (boostMode?.indexOf('^') > -1) {
            return boostMode.tokenize(',')
        }
        if (boostMode == 'id.kb.se') {
            return CONCEPT_BOOST
        }

	Map<String, List<String>> boostFieldsByType = [:]

        String typeKey = types != null ? types.toUnique().sort().join(',') : ''
        typeKey += boostMode

        List<String> boostFields = boostFieldsByType[typeKey]
        if (boostFields == null) {
            if (boostMode == 'hardcoded') {
                boostFields = [
                    'prefLabel^100',
                    'code^100',
                    'name^100',
                    'familyName^100', 'givenName^100',
                    'lifeSpan^100', 'birthYear^100', 'deathYear^100',
                    'hasTitle.mainTitle^100', 'title^100',
                    'heldBy.sigel^100',
                ]
            } else {
                boostFields = computeBoostFields(types, lensBoost)
            }
            boostFieldsByType[typeKey] = boostFields
        }
}

List<String> computeBoostFields(String[] types, ESQueryLensBoost lensBoost) {
        /* FIXME:
           lensBoost.computeBoostFieldsFromLenses does not give a good result for Concept. 
           Use hand-tuned boosting instead until we improve boosting/ranking in general. See LXL-3399 for details. 
        */
        def l = ((types ?: []) as List<String>).split { jsonld.isSubClassOf(it, 'Concept') }
        def (conceptTypes, otherTypes) = [l[0], l[1]]
        
        if (conceptTypes) {
            if (otherTypes) {
                def fromLens = lensBoost.computeBoostFieldsFromLenses(otherTypes as String[])
                def conceptFields = CONCEPT_BOOST.collect{ it.split('\\^')[0]}
                def otherFieldsBoost = fromLens.findAll{!conceptFields.contains(it.split('\\^')[0]) }
                return CONCEPT_BOOST + otherFieldsBoost
            }
            else {
                return CONCEPT_BOOST
            }
        }
        else {
            return lensBoost.computeBoostFieldsFromLenses(types)
        }
}
        
private static final List<String> CONCEPT_BOOST = [
            'prefLabel^1500',
            'prefLabelByLang.sv^1500',
            'label^500',
            'labelByLang.sv^500',
            'code^200',
            'termComponentList._str.exact^125',
            'termComponentList._str^75',
            'altLabel^150',
            'altLabelByLang.sv^150',
            'hasVariant.prefLabel.exact^150',
            '_str.exact^100',
            'inScheme._str.exact^100',
            'inScheme._str^100',
            'inCollection._str.exact^10',
            'broader._str.exact^10',
            'exactMatch._str.exact^10',
            'closeMatch._str.exact^10',
            'broadMatch._str.exact^10',
            'related._str.exact^10',
            'scopeNote^10',
            'keyword._str.exact^10',
]

/**
     * Expand `@type` query parameter with subclasses.
     *
     * This also removes superclasses, since we only care about the most
     * specific class.
*/

static String[] expandTypeParam(String[] types, JsonLd jsonld) {
        // Filter out all types that have (more specific) subclasses that are
        // also in the list.
        // So for example [Instance, Electronic] should be reduced to just
        // [Electronic].
        // Afterwards, include all subclasses of the remaining types.
        Set<String> subClasses = []

        // Select types to prune
        Set<String> toBeRemoved = []
        for (String c1 : types) {
            ArrayList<String> c1SuperClasses = []
            jsonld.getSuperClasses(c1, c1SuperClasses)
            toBeRemoved.addAll(c1SuperClasses)
        }
        // Make a new pruned list without the undesired superclasses
        List<String> prunedTypes = []
        for (String type : types) {
            if (!toBeRemoved.contains(type))
                prunedTypes.add(type)
        }
        // Add all subclasses of the remaining types
        for (String type : prunedTypes) {
            subClasses += jsonld.getSubClasses(type)
            subClasses.add(type)
        }

        return subClasses.toArray()
}

}
