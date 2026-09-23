package whelk.converter.marc

import spock.lang.Specification

class BibTypeNormalizationStepSpec extends Specification {

  static Map testCategories = [
    '_:gf1': ['@id': '_:gf1', '@type': 'GenreForm'],
    '_:gf2': ['@id': '_:gf2', '@type': 'GenreForm', 'broader': [['@id': '_:gf1']]],
    '_:gf3': ['@id': '_:gf3', '@type': 'GenreForm', 'closeMatch': [['@id': '_:gf4']]],
    '_:gf4': ['@id': '_:gf4', '@type': 'GenreForm', 'closeMatch': [['@id': '_:gf3']]],  // cycle!
    '_:gf5': ['@id': '_:gf5', '@type': 'GenreForm'],
  ]

  // subclass to overcome too coupled components (JsonLd and ResourceCache)
  static var bibTypeNormalizationStep = new BibTypeNormalizationStep() {
    boolean isSubClassOf(Object givenType, String baseType) {
        return givenType == baseType
    }
    List<Map<String, Object>> getDescriptions(Object refs, boolean onlyLinked=false) {
        return refs.collect { testCategories[it['@id']] }
    }
  }

  static {
    bibTypeNormalizationStep.matchRelations = ['broader', 'closeMatch']
    bibTypeNormalizationStep.prioritizedWorkLegacyTypes = ['Multimedia', 'Text']
    bibTypeNormalizationStep.marcTypeMappings = [:]
    bibTypeNormalizationStep.categoryTypeMap = [
        '_:gf3': 'Text',
        '_:gf5': 'Image',
    ]
  }

  def "should collect categories by type"() {
      given:
      def workCategories = testCategories.values() as List
      when:
      def categories = bibTypeNormalizationStep.getCategoryOfType(workCategories, 'GenreForm', true)
      def impliedTypes = bibTypeNormalizationStep.collectImpliedTypesFromCategory(categories)
      then:
      impliedTypes == ['Text', 'Image'] as Set
      and:
      bibTypeNormalizationStep.getWorkType(workCategories) == 'Text'
  }
}
