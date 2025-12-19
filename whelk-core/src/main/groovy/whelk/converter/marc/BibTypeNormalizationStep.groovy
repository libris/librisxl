package whelk.converter.marc

//import groovy.transform.CompileStatic

import whelk.TypeCategoryNormalizer
import static whelk.JsonLd.asList

//@CompileStatic
class BibTypeNormalizationStep extends MarcFramePostProcStepBase {

    boolean requiresResources = true

    TypeCategoryNormalizer catTypeNormalizer

    Set<String> issuanceTypes = new HashSet()
    Map<String, String> categoryTypeMap = new HashMap()
    List<String> matchRelations = ['exactMatch', 'closeMatch', 'broader', 'broadMatch']

    void init() {
        if (!checkRequired()) {
            return
        }

        catTypeNormalizer = new TypeCategoryNormalizer(resourceCache)
        issuanceTypes.addAll(['Monograph', 'Serial', 'Collection', 'Integrating'])
        // 'SingleUnit', 'MultipleUnits'
        catTypeNormalizer.typeToCategory.each { type, cat ->
            categoryTypeMap[cat] = type
        }
    }

    boolean checkRequired() {
        if (resourceCache == null) {
            // FIXME: log.warn
            System.err.println("BibTypeNormalizationStep MISSING resourceCache!")
            return false
        }
        return true
    }

    void modify(Map record, Map thing) {
        // FIXME: apply typeNormalizer
    }

    void unmodify(Map record, Map instance) {
        if (!checkRequired()) {
            return
        }

        def work = instance.instanceOf
        def issuanceType = getIssuanceType(work)
        toLegacyWork(work, instance)
        toLegacyInstance(instance, work)
        instance.issuanceType = issuanceType
    }

    void toLegacyWork(Map work, Map someInstance) {
        def categories = getDescriptions(work.category)
        work.remove('category')
        work.contentType = getCategoryOfType(categories, 'ContentType')
        work.genreForm = getCategoryOfType(categories, 'GenreForm')
        work[TYPE] = getImpliedTypeFromCategory(categories)
        // FIXME: ManuscriptText | ManuscriptNotatedMusic | Cartography
    }

    void toLegacyInstance(Map instance, Map work) {
        def categories = getDescriptions(instance.category)
        instance.remove('category')
        instance.mediaType = getCategoryOfType(categories, 'MediaType')
        instance.carrierType = getCategoryOfType(categories, 'CarrierType')
        instance[TYPE] = getImpliedTypeFromCategory(categories)
        // FIXME: reverse the hardcoded (typenorm script) cleanupInstanceTypes too!
    }

    List<Map<String, Object>> getDescriptions(Object refs) {
        return (List<Map<String, Object>>) asList(refs).findResults {
            catTypeNormalizer.categories[it[ID]]
        }
    }

    String getIssuanceType(Map work) {
        for (def type in asList(work[TYPE])) {
            if (type in issuanceTypes) {
                return type
            }
        }
        return null
    }

    Collection<Map<String, Object>> getCategoryOfType(List<Map<String, Object>> categories, String type) {
        def result = [:]
        collectCategoryOfType(categories, type, result)
        return result.values().toList()
    }

    void collectCategoryOfType(List<Map<String, Object>> categories, String type, Map<String, Map<String, Object>> result) {
        categories.each {
            if (resourceCache.jsonld.isSubClassOf(it[TYPE], type)) {
                result[it[ID]] = it
            }
            for (rel in matchRelations) {
                collectCategoryOfType(getDescriptions(it[rel]), type, result)
            }
        }
    }

    String getImpliedTypeFromCategory(List<Map<String, Object>> categories) {
        // TODO: if multiple types, select one (e.g. prefer Text over StillImage?)
        def type = categories.findResult { categoryTypeMap[it[ID]] }
        if (type) {
            return type
        }
        // breadth first search, assuming matchRelations are ordered by closeness
        // (but less optimal for lots of categories)
        for (rel in matchRelations) {
            for (category in categories) {
                type = getImpliedTypeFromCategory(getDescriptions(category[rel]))
                if (type) {
                    return type
                }
            }
        }
    }

}
