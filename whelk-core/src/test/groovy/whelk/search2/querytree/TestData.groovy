package whelk.search2.querytree

import whelk.JsonLd
import whelk.search2.Disambiguate
import whelk.search2.Filter
import whelk.search2.VocabMappings

class TestData {
    static def getDisambiguate() {
        def propertyAliasMappings = [
                'p1'  : 'p1',
                'p2'  : 'p2',
                'p3'  : 'p3',
                'p4'  : 'p4',
                'type': 'rdf:type'
        ]
        def classAliasMappings = [
                't1': 'T1',
                't2': 'T2'
        ]
        def enumAliasMappings = [
                'e1': 'E1'
        ]
        def vocabMappings = new VocabMappings([
                "propertyAliasMappings": propertyAliasMappings,
                "classAliasMappings"   : classAliasMappings,
                "enumAliasMappings"    : enumAliasMappings
        ])
        def filterAliases = [
                new Filter.AliasedFilter("excludeA", "NOT p1:A", [:]),
                new Filter.AliasedFilter("includeA", "NOT excludeA", [:]),
        ]

        return new Disambiguate(vocabMappings, filterAliases, getJsonLd())
    }

    static def getJsonLd() {
        def vocab = ['@graph': [
                ['@id': 'p1', '@type': 'DatatypeProperty'],
                ['@id': 'p2', '@type': 'ObjectProperty'],
                ['@id': 'p3', '@type': 'ObjectProperty'],
                ['@id': 'p4', '@type': 'ObjectProperty'],
                ['@id': 'textQuery', '@type': 'DatatypeProperty'],
                ['@id': 'rdf:type', '@type': 'ObjectProperty'],
                ['@id': 'T1', '@type': 'Class'],
                ['@id': 'T2', '@type': 'Class'],
                ['@id': 'E1', '@type': 'Class']
        ]]
        def ctx = [
                '@context': ['@vocab': 'https://id.kb.se/vocab/', 'p2': ['@type': '@vocab']]
        ]
        return new JsonLd(ctx, [:], vocab)
    }
}
