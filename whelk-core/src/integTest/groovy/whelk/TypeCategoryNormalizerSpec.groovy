package whelk

import spock.lang.Ignore
import spock.lang.Specification

import whelk.JsonLd
import whelk.ResourceCache
import whelk.TypeCategoryNormalizer

import whelk.converter.marc.MarcFrameCli

import static whelk.JsonLd.ID_KEY as ID
import static whelk.util.Jackson.mapper

class TypeCategoryNormalizerSpec extends Specification {

    static ResourceCache resourceCache
    static {
      JsonLd jsonld = null
      if (System.properties.getProperty('xl.secret.properties')) {
          def whelk = Whelk.createLoadedCoreWhelk()
          jsonld = whelk.jsonld
          resourceCache = whelk.resourceCache
      } else {
        var defsBuildDir = System.properties.get('xl.definitions.builddir')?: '../../definitions/build'
        jsonld = MarcFrameCli.getLocalJsonLd(defsBuildDir)

        resourceCache = new ResourceCache(jsonld)
        var resourceCacheDir = System.properties.get('xl.resourcecache.dir') ?: '../../xl-resource-cache'
        var byTypeCacheFile = new File("${resourceCacheDir}/typecache.json")
        assert byTypeCacheFile.exists()
        resourceCache.byTypeCache = mapper.readValue(byTypeCacheFile, Map)
      }
    }

    def normalizer = new TypeCategoryNormalizer(resourceCache)

    def "term is implied by a broader term"() {
        expect:
        normalizer.isImpliedBy([(ID): 'https://id.kb.se/term/rda/Unmediated'], [(ID): 'https://id.kb.se/term/rda/Volume'])
        normalizer.isImpliedBy([(ID): 'https://id.kb.se/term/saobf/AbstractElectronic'], [(ID): 'https://id.kb.se/term/rda/ComputerDiscCartridge'])
        normalizer.isImpliedBy([(ID): 'https://id.kb.se/term/saobf/AbstractElectronic'], [(ID): 'https://id.kb.se/term/rda/OnlineResource'])
    }

    def "terms are reduced to a set of most specific"() {
        expect:
        normalizer.reduceSymbols([[(ID): 'https://id.kb.se/term/rda/Unmediated'], [(ID): 'https://id.kb.se/term/rda/Volume']]) == [[(ID): 'https://id.kb.se/term/rda/Volume']]
    }
}
