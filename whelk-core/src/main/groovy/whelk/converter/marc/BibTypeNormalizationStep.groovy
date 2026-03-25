package whelk.converter.marc

//import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log

import whelk.TypeCategoryNormalizer
import whelk.converter.BibTypeNormalizer
import static whelk.converter.BibTypeNormalizer.getType
import whelk.util.DocumentUtil

import static whelk.JsonLd.asList

//@CompileStatic
@Log
class BibTypeNormalizationStep extends MarcFramePostProcStepBase {

    boolean requiresResources = true

    BibTypeNormalizer bibTypeNormaliser

    // Injected configuration
    List<String> matchRelations = Collections.emptyList()
    List<String> newWorkTypes = Collections.emptyList()
    String defaultWorkLegacyType
    String componentPartCategory
    List<String> prioritizedWorkLegacyTypes = Collections.emptyList()
    List<String> keepMatchingWithCategories = Collections.emptyList()

    Map<String, Set<String>> marcTypeMappings  // TODO: turn JSON set value to Set! (for speed)

    Set<String> issuanceTypeSet = new HashSet()
    Map<String, String> categoryTypeMap = new HashMap()

    Set<String> keepMatchingWithCategoriesSet = new HashSet()

    void init() {
        if (!checkRequired()) {
            return
        }

        bibTypeNormaliser = new BibTypeNormalizer(resourceCache)

        issuanceTypeSet.addAll(newWorkTypes)

        typeCategoryNormalizer.typeToCategory.each { type, cat ->
            categoryTypeMap[cat] = type
        }

        keepMatchingWithCategoriesSet.addAll(keepMatchingWithCategories)
    }

    TypeCategoryNormalizer getTypeCategoryNormalizer() {
        return bibTypeNormaliser.normalizer
    }

    boolean checkRequired() {
        if (resourceCache == null) {
            log.warn("BibTypeNormalizationStep MISSING resourceCache!")
            return false
        }

        return true
    }

    void modify(Map record, Map thing) {
        if (!checkRequired()) {
            return
        }

        bibTypeNormaliser.normalize(thing, thing.instanceOf)
    }

    void unmodify(Map record, Map instance) {
        if (!checkRequired()) {
            return
        }

        if (instance.instanceOf == null) {
            log.debug("Does not look like an instance (cannot usefully unmodify); skipping")
            return
        }

        denormalize(instance)
    }

    void denormalize(Map instance) {
        def work = instance.get('instanceOf', [:]) // NOTE: *sets* default value

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
        reshapeToLegacyWork(work, workCategories)
        reshapeToLegacyInstance(instance, workCategories, instanceCategories)
        instance.issuanceType = issuanceType ?: 'Monograph'

        DocumentUtil.traverse(instance) { value, path ->
            if (!path.isEmpty() && !path.contains('instanceOf')) {
                if (value instanceof Map && resourceCache.jsonld.isSubClassOf(getType(value), 'Instance')) {
                    denormalize(value)
                }
            }
        }

        DocumentUtil.traverse(work) { value, path ->
            if (!path.isEmpty()) {
                if (value instanceof Map && resourceCache.jsonld.isSubClassOf(getType(value), 'Work')) {
                    reshapeToLegacyWork(value, getDescriptions(value.remove('category')))
                }
            }
        }

    }


    void reshapeToLegacyWork(Map work, List workCategories) {
        work.contentType = getCategoryOfType(workCategories, 'ContentType', true)
        work.genreForm = getCategoryOfType(workCategories, 'GenreForm')
        work[TYPE] = getWorkType(workCategories) ?: defaultWorkLegacyType
    }

    String getWorkType(List<Map<String, Object>> workCategories) {
        // TODO:
        // - More involved match ranking? i:Audio [if SpokenWords?/Ljudböcker] > a:Text > k:StillImage ?
        // - also construct ManuscriptText | ManuscriptNotatedMusic | Cartography (if asked for)
        var result = collectImpliedTypesFromCategory(workCategories)
        for (prioType in prioritizedWorkLegacyTypes) {
            if (prioType in result) {
                return prioType
            }
        }
        for (type in result) return type
    }

    void reshapeToLegacyInstance(Map instance, List workCategories, List instanceCategories) {
        instance.mediaType = getCategoryOfType(instanceCategories, 'MediaType', true)
        instance.carrierType = getCategoryOfType(instanceCategories, 'CarrierType', true)

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

    List<Map<String, Object>> getDescriptions(Object refs, boolean onlyLinked=false) {
        return (List<Map<String, Object>>) asList(refs).findResults {
            ID in it && it[ID] in typeCategoryNormalizer.categories
                ? new HashMap(typeCategoryNormalizer.categories[it[ID]])
                : onlyLinked ? null : it
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

    boolean isComplexSubjectWithFirstTermOfType(Map term, String baseType) {
        if (term[TYPE] == 'ComplexSubject') {
            def termComponentList = asList(term['termComponentList'])
            if (termComponentList.size() > 0) {
                return isSubClassOf(termComponentList[0][TYPE], type)
            }
        }
        return false
    }

    // TODO: optimize by returning result *Map* (check only id in keys unless more is needed)
    Collection<Map<String, Object>> getCategoryOfType(List<Map<String, Object>> categories, String type, boolean keepImplied=false) {
        var result = new LinkedHashMap()
        collectCategoryOfType(categories, type, keepImplied, result)
        return result.values().toList()
    }

    private void collectCategoryOfType(List<Map<String, Object>> categories, String type, boolean keepImplied, Map<String, Map<String, Object>> result) {
        for (Map<String, Object> ctg : categories) {
            if (
                asList(ctg[TYPE]).any { t -> isSubClassOf(t, type) }
                || isComplexSubjectWithFirstTermOfType(ctg, type)
            ) {
                def key = ctg[ID] ?: '_:b' + result.size().toString() // id or throwaway fake blank id
                result[key] = ctg
            }
            for (rel in matchRelations) {
                var implied = getDescriptions(ctg[rel], true)
                if (!keepImplied) {
                    implied.each {
                        var keep = ID in it
                            && keepMatchingWithCategoriesSet
                            && asList(it['inCollection']).any { it[ID] in keepMatchingWithCategoriesSet }
                        if (!keep) {
                            // Make MarcFrameConverter skip these for regular fields
                            it['_revertedBy'] = 'BibTypeNormalizationStep'
                        }
                    }
                }
                collectCategoryOfType(implied, type, keepImplied, result)
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
