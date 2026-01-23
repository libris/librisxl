package whelk.converter.marc

//import groovy.transform.CompileStatic

import whelk.TypeCategoryNormalizer
import static whelk.JsonLd.asList

//@CompileStatic
class BibTypeNormalizationStep extends MarcFramePostProcStepBase {

    boolean requiresResources = true

    TypeCategoryNormalizer catTypeNormalizer

    // Injected configuration
    List<String> matchRelations
    List<String> newWorkTypes
    String componentPartCategory
    Map<String, Set<String>> marcTypeMappings  // TODO: turn JSON set value to Set! (for speed)

    Set<String> issuanceTypeSet = new HashSet()
    Map<String, String> categoryTypeMap = new HashMap()

    void init() {
        if (!checkRequired()) {
            return
        }

        catTypeNormalizer = new TypeCategoryNormalizer(resourceCache)
        issuanceTypeSet.addAll(newWorkTypes)

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
        work[TYPE] = getWorkType(workCategories) ?: 'Text'
    }

    String getWorkType(List<Map<String, Object>> workCategories) {
        // FIXME:
        // - rank possible matches! i:Audio [if SpokenWords?/Ljudböcker] > a:Text > k:StillImage ?
        // - also figure out: ManuscriptText | ManuscriptNotatedMusic | Cartography
        return getImpliedTypeFromCategory(workCategories)
    }

    void reshapeToLegacyInstance(Map work, Map instance, List workCategories, List instanceCategories) {
        instance.mediaType = getCategoryOfType(instanceCategories, 'MediaType')
        instance.carrierType = getCategoryOfType(instanceCategories, 'CarrierType')

        // TODO: until we've finalized InstanceGenreForm->ManifestationForm
        instance.genreForm = getCategoryOfType(instanceCategories, 'GenreForm')
        instance[TYPE] = getInstanceType(instanceCategories) ?: 'Instance'
    }

    String getInstanceType(List<Map<String, Object>> instanceCategories) {
        // FIXME: reverse the hardcoded (typenorm script) cleanupInstanceTypes too!
        // Either look at work-category, some matches 007-genreForm-terms ...
        //
        // - :Electronic>	9925536  // LAST
        //      - TODO: specify expected; e.g: bib 007: cr
        // - :VideoRecording>	328175
        // - :SoundRecording>	185064
        // - :StillImageInstance>	55997
        // - :Tactile>	20428
        // - :Map>	12996
        // - :Microform>	1969
        // - :Globe>	92
        //
        // Strays:
        // - :TextInstance>	2
        // 0:
        // - :ProjectedImageInstance	0!
        // - :MovingImageInstance	o!

        return getImpliedTypeFromCategory(instanceCategories)
    }

  List<Map<String, Object>> getDescriptions(Object refs) {
        return (List<Map<String, Object>>) asList(refs).findResults {
            if (ID in it) catTypeNormalizer.categories[it[ID]]?.clone() // TODO: tries as lazy-copies?
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
    // - also: LinkedHashMap, ordered by most specific (broader last; also see catTypeNormalizer.preferredCategory?)
    // - Note that MarcFrameConverter now uses a fixed field preference order though (based on matchUriToken).
    Collection<Map<String, Object>> getCategoryOfType(List<Map<String, Object>> categories, String type) {
        var result = new LinkedHashMap()
        collectCategoryOfType(categories, type, result)
        return result.values().toList()
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
        return null
    }

}
