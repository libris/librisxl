package whelk.converter.marc

import spock.lang.Specification

import whelk.JsonLd
import whelk.ResourceCache

class BibTypeNormalizationStepSpec extends Specification {

  static Map testCategories = [
    '_:gf1': ['@id': '_:gf1', '@type': 'GenreForm'],
    '_:gf2': ['@id': '_:gf2', '@type': 'GenreForm', 'broader': [['@id': '_:gf1']]],
    '_:gf3': ['@id': '_:gf3', '@type': 'GenreForm', 'closeMatch': [['@id': '_:gf4']]],
    '_:gf4': ['@id': '_:gf4', '@type': 'GenreForm', 'closeMatch': [['@id': '_:gf3']]],  // cycle!
    '_:gf5': ['@id': '_:gf5', '@type': 'GenreForm'],
  ]

  // minimal vocab so the traversals in denormalize() can resolve Work/Instance
  static Map VOCAB = ['@graph': [
    ['@id': 'https://id.kb.se/vocab/Work', '@type': 'Class'],
    ['@id': 'https://id.kb.se/vocab/Instance', '@type': 'Class'],
    ['@id': 'https://id.kb.se/vocab/Monograph', '@type': 'Class',
     'subClassOf': [['@id': 'https://id.kb.se/vocab/Work']]],
  ]]
  static Map CONTEXT = ['@context': ['@vocab': 'https://id.kb.se/vocab/']]

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
    bibTypeNormalizationStep.resourceCache = new ResourceCache(new JsonLd(CONTEXT, [:], VOCAB))
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

  def "should denormalize an instance with a list of works"() {
      given:
      def instance = [
        '@type': 'Instance',
        'category': [['@id': '_:gf5']],
        'instanceOf': [
          ['@type': 'Work',
           'category': [['@id': '_:gf3']],
           'hasTitle': [['@type': 'Title', 'mainTitle': 'Darth Bane series']]],
          ['@type': 'Work',
           'hasTitle': [['@type': 'Title', 'mainTitle': 'Star wars']]],
        ],
      ]
      when:
      bibTypeNormalizationStep.denormalize(instance)
      then:
      noExceptionThrown()
      and: 'both works are reshaped to legacy form'
      instance.instanceOf.every { it.containsKey('contentType') && it.containsKey('genreForm') }
      instance.instanceOf*.'@type' == ['Text', 'Text']
      and: 'categories are removed and issuanceType set'
      instance.instanceOf.every { !it.containsKey('category') }
      !instance.containsKey('category')
      instance.issuanceType == 'Monograph'
  }

  def "should denormalize a nested series instance whose works are a list"() {
      given: 'the shape of libris.kb.se/n6jv280rl5s01mj6, where seriesMembership.inSeries has two works'
      def inSeries = [
        '@type': 'Instance',
        'instanceOf': [
          ['@type': 'Work',
           'hasTitle': [['@type': 'Title', 'mainTitle': 'Darth Bane series']],
           'contribution': [['@type': 'PrimaryContribution',
                             'agent': ['@type': 'Person',
                                       'givenName': 'Drew.',
                                       'familyName': 'Karpyshyn']]]],
          ['@type': 'Work',
           'hasTitle': [['@type': 'Title', 'mainTitle': 'Star wars']]],
        ],
      ]
      def instance = [
        '@type': 'Instance',
        'category': [['@id': '_:gf5']],
        'instanceOf': ['@type': 'Work', 'category': [['@id': '_:gf3']]],
        'seriesMembership': [
          ['@type': 'SeriesMembership', 'inSeries': inSeries, 'seriesEnumeration': '1.'],
        ],
      ]
      when:
      bibTypeNormalizationStep.denormalize(instance)
      then:
      noExceptionThrown()
      and: 'the nested instance has no categories, so it is left as it was'
      inSeries['@type'] == 'Instance'
      inSeries.instanceOf.size() == 2
      inSeries.instanceOf*.'@type' == ['Work', 'Work']
      !inSeries.containsKey('issuanceType')
  }
}
