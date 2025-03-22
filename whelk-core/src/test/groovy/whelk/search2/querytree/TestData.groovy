package whelk.search2.querytree

import whelk.JsonLd
import whelk.search2.Disambiguate
import whelk.search2.Filter
import whelk.search2.VocabMappings

class TestData {
    static def getDisambiguate() {
        def propertyAliasMappings = ['p': 'p']
        def vocabMappings = new VocabMappings(["propertyAliasMappings": propertyAliasMappings])
        def filterAliases = [
                new Filter.AliasedFilter("excludeA", "NOT p:A", [:]),
                new Filter.AliasedFilter("includeA", "NOT excludeA", [:]),
        ]

        return new Disambiguate(vocabMappings, filterAliases, getJsonLd())
    }

    static def getJsonLd() {
        def vocab = ['@graph': [['@id':'p', '@type':'DatatypeProperty']]]
        return new JsonLd([:], [:], vocab)
    }
}
