package whelk.converter.marc

//import groovy.transform.CompileStatic

import whelk.TypeCategoryNormalizer
import whelk.converter.BibTypeNormalizer

import static whelk.JsonLd.asList

//@CompileStatic
class BibTypeNormalizationStep extends MarcFramePostProcStepBase {

    boolean requiresResources = true

    BibTypeNormalizer bibTypeNormaliser

    // Injected configuration
    List<String> matchRelations
    List<String> newWorkTypes
    String defaultWorkLegacyType
    String componentPartCategory
    String abstractTermCategory
    Map<String, Set<String>> marcTypeMappings  // TODO: turn JSON set value to Set! (for speed)

    Set<String> issuanceTypeSet = new HashSet()
    Map<String, String> categoryTypeMap = new HashMap()

    void init() {
        if (!checkRequired()) {
            return
        }

        bibTypeNormaliser = new BibTypeNormalizer(resourceCache)

        issuanceTypeSet.addAll(newWorkTypes)

        typeCategoryNormalizer.typeToCategory.each { type, cat ->
            categoryTypeMap[cat] = type
        }
    }

    TypeCategoryNormalizer getTypeCategoryNormalizer() {
        return bibTypeNormaliser.normalizer
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
        bibTypeNormaliser.normalize(thing, thing.instanceOf)
    }

    void unmodify(Map record, Map instance) {
        if (!checkRequired()) {
            return
        }

        def work = instance.instanceOf

        // Pick out the categories:
        var workCategories = getDescriptions(work.category)
        var instanceCategories = getDescriptions(instance.category)

        // Stop processing if we already seem to have a legacy shape:
        // TODO: define more detailed "looks like legacy"?
        if (!workCategories && !instanceCategories) { // && workType !in newWorkTypes ?
          return
        }

        // Then remove from shape (*might* hurt to keep, since these are to be used in legacy form):
        work.remove('category')
        instance.remove('category')

        // Mutate into legacy form:
        def issuanceType = getIssuanceType(work[TYPE], instance[TYPE], workCategories, instanceCategories)
        reshapeToLegacyWork(work, instance, workCategories, instanceCategories)
        reshapeToLegacyInstance(work, instance, workCategories, instanceCategories)
        instance.issuanceType = issuanceType ?: 'Monograph'
    }

    void reshapeToLegacyWork(Map work, Map someInstance, List workCategories, List instanceCategories) {
        work.contentType = getCategoryOfType(workCategories, 'ContentType')
        work.genreForm = getCategoryOfType(workCategories, 'GenreForm')
        work[TYPE] = getWorkType(workCategories) ?: defaultWorkLegacyType
    }

    String getWorkType(List<Map<String, Object>> workCategories) {
        // TODO:
        // - better match ranking? i:Audio [if SpokenWords?/Ljudböcker] > a:Text > k:StillImage ?
        // FIXME: also construct ManuscriptText | ManuscriptNotatedMusic | Cartography
        var result = collectImpliedTypesFromCategory(workCategories)
        if (defaultWorkLegacyType in result) return defaultWorkLegacyType
        for (type in result) return type
    }

    void reshapeToLegacyInstance(Map work, Map instance, List workCategories, List instanceCategories) {
        instance.mediaType = getCategoryOfType(instanceCategories, 'MediaType')
        instance.carrierType = getCategoryOfType(instanceCategories, 'CarrierType')

        // TODO: Keeping for now; until we've finalized InstanceGenreForm->ManifestationForm?
        instance.genreForm = getCategoryOfType(instanceCategories, 'GenreForm')
        var itype = getInstanceType(instanceCategories)

        if (itype == null) {
            typeCategoryNormalizer.remappedLegacyInstanceTypes.each { ruletype, rule ->
                if (typeCategoryNormalizer.anyImplies(instanceCategories, rule.category)) {
                    if (typeCategoryNormalizer.anyImplies(workCategories, rule.workCategory)) {
                        itype = ruletype
                    }
                }
            }
        }

        instance[TYPE] = itype ?: 'Instance'
    }

    String getInstanceType(List<Map<String, Object>> instanceCategories) {
        var result = collectImpliedTypesFromCategory(instanceCategories)
        for (type in result) return type
    }

  List<Map<String, Object>> getDescriptions(Object refs) {
        return (List<Map<String, Object>>) asList(refs).findResults {
            if (ID in it) typeCategoryNormalizer.categories[it[ID]]
        }
    }

    String getIssuanceType(String workType, String instanceType, List workCategories, List instanceCategories) {
        if (instanceCategories.any { it[ID] == componentPartCategory}) {
          return 'ComponentPart'
        }
        for (def type in asList(workType)) {
            if (type in issuanceTypeSet) {
                return type
            }
        }
        return null
    }

    // TODO: optimize by returning result *Map* (check only id in keys unless more is needed)
    // - also: LinkedHashMap, ordered by most specific (broader last; also see typeCategoryNormalizer.preferredCategory?)
    // - Note that MarcFrameConverter now uses a fixed field preference order though (based on matchUriToken).
    Collection<Map<String, Object>> getCategoryOfType(List<Map<String, Object>> categories, String type) {
        var result = new LinkedHashMap()
        collectCategoryOfType(categories, type, result)
        return result.values().findResults { ctg ->
            if (!abstractTermCategory || !asList(ctg['category']).any { it[ID] == abstractTermCategory}) {
                new HashMap(ctg)
            }
        }
    }

    boolean isSubClassOf(Object givenType, String baseType) {
      if (givenType instanceof List) {
        for (Object type : givenType) {
          if (isSubClassOf(type, baseType)) {
            return true
          }
        }
      }

      if (givenType !instanceof String) {
        return false
      }

      // Hardcoded "ducktyped" marc enum types (which used to be real subclasses):
      Set marcTypes = marcTypeMappings[baseType]
      if (marcTypes != null && marcTypes.contains(givenType)) {
        return true
      }

      return resourceCache.jsonld.isSubClassOf(givenType, baseType)
    }

    private void collectCategoryOfType(List<Map<String, Object>> categories, String type, Map<String, Map<String, Object>> result) {
        categories.each {
            if (asList(it[TYPE]).any { t -> isSubClassOf(t, type) }) {
                result[it[ID]] = it
            }
            for (rel in matchRelations) {
                collectCategoryOfType(getDescriptions(it[rel]), type, result)
            }
        }
    }

    Set<String> collectImpliedTypesFromCategory(List<Map<String, Object>> categories) {
      var result = new HashSet<String>()
      collectImpliedTypesFromCategory(categories, result)
      return result
    }
    void collectImpliedTypesFromCategory(List<Map<String, Object>> categories, Set<String> result) {
        // TODO: if multiple types, select one (e.g. prefer Text over StillImage?)
        for (ctg in categories) {
          var type = categoryTypeMap[ctg[ID]]
          if (type) {
              result << type
          }
        }
        // breadth first search, assuming matchRelations are ordered by closeness
        // (but less optimal for lots of categories)
        for (rel in matchRelations) {
            for (category in categories) {
                collectImpliedTypesFromCategory(getDescriptions(category[rel]), result)
            }
        }
    }

}
