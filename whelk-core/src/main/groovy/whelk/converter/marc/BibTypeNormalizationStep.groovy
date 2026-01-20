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


    Map<String, Set<String>> marcTypeMappings

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
        var converted = toLegacyWork(work, instance)
        converted |= toLegacyInstance(instance, work)
        if (converted) {
          instance.issuanceType = issuanceType ?: 'Monograph'
        }
    }

    boolean toLegacyWork(Map work, Map someInstance) {
        def categories = getDescriptions(work.category)
        if (!categories) {
          // TODO: define more detailed "looks like legacy"
          return false
        }

        work.remove('category')
        work.contentType = getCategoryOfType(categories, 'ContentType')
        work.genreForm = getCategoryOfType(categories, 'GenreForm')
        work[TYPE] = getWorkType(categories) ?: 'Text'

        return true
    }

    String getWorkType(List<Map<String, Object>> categories) {
        // FIXME:
        // - rank possible matches! i:Audio [if SpokenWords?/Ljudböcker] > a:Text > k:StillImage ?
        // - also figure out: ManuscriptText | ManuscriptNotatedMusic | Cartography
        return getImpliedTypeFromCategory(categories)
    }

    boolean toLegacyInstance(Map instance, Map work) {
        def categories = getDescriptions(instance.category)
        if (!categories) {
          // TODO: define more detailed "looks like legacy"
          return false
        }

        instance.remove('category')
        instance.mediaType = getCategoryOfType(categories, 'MediaType')
        instance.carrierType = getCategoryOfType(categories, 'CarrierType')

        // TODO: until we've finalized InstanceGenreForm->ManifestationForm
        instance.genreForm = getCategoryOfType(categories, 'GenreForm')
        instance[TYPE] = getInstanceType(categories)

        return true
    }

    String getInstanceType(List<Map<String, Object>> categories) {
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
        return getImpliedTypeFromCategory(categories)
    }

    List<Map<String, Object>> getDescriptions(Object refs) {
        return (List<Map<String, Object>>) asList(refs).findResults {
            if (ID in it) catTypeNormalizer.categories[it[ID]]
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

    boolean isSubClassOf(Object givenType, String baseType) {
      if (baseType instanceof List) {
        for (Object type : baseType) {
          if (isSubClassOf(type, baseType)) {
            return true
          }
        }
      }
      Set marcTypes = marcTypeMappings[baseType]
      if (marcTypes != null && marcTypes.contains(givenType)) {
        return true
      }
      return resourceCache.jsonld.isSubClassOf(givenType, type)
    }

    void collectCategoryOfType(List<Map<String, Object>> categories, String type, Map<String, Map<String, Object>> result) {
        categories.each {
            if (isSubClassOf(it[TYPE], type)) {
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
