package whelk.xlql

import spock.lang.Ignore
import spock.lang.Specification
import whelk.Whelk
import whelk.search2.Disambiguate
import whelk.search2.Path
import whelk.search2.querytree.QueryTree

// Requires Whelk instance
@Ignore
class DisambiguateSpec extends Specification {
    private static Whelk whelk = Whelk.createLoadedSearchWhelk();
    private static Disambiguate disambiguate = new Disambiguate(whelk);

    def "map alias to kbv property"() {
        expect:
        disambiguate.mapToProperty(alias) == Optional.ofNullable(property)

        where:
        alias                            | property
        "subject"                        | "subject"
        "ämne"                           | "subject"
        "https://id.kb.se/vocab/subject" | "subject"
        "kbv:subject"                    | "subject"
        "dc:subject"                     | "subject"
        "pbl"                            | "publisher"
        "translation of"                 | "translationOf"
        "translationof"                  | "translationOf"
        "översättning av"                | "translationOf"
        "förf"                           | "author"
        "unknown term"                   | null
        "marc:jurisdiction"              | "marc:jurisdiction"
        "jurisdiction"                   | null // require marc: prefix for marc properties
        "type"                           | "rdf:type"
        "format"                         | "format"
        "hasformat"                      | "hasFormat"
    }

    def "map alias to kbv class"() {
        expect:
        disambiguate.mapToKbvClass(alias) == Optional.ofNullable(kbvClass)

        where:
        alias                                 | kbvClass
        "Bibliografi"                         | "Bibliography"
        "bibliography"                        | "Bibliography"
        "kbv:bibliography"                    | "Bibliography"
        "https://id.kb.se/vocab/bibliography" | "Bibliography"
        "unknown term"                        | null
        "text"                                | "Text"
        "topic"                               | "Topic"
        "allmänt ämnesord"                    | "Topic"
        "agent"                               | "Agent"
        "bibliographicagent"                  | "BibliographicAgent"
    }

    def "map alias to kbv enum class"() {
        expect:
        disambiguate.mapToEnum(alias) == Optional.ofNullable(enumClass)

        where:
        alias                                    | enumClass
        "marc:AbbreviatedLevel"                  | "marc:AbbreviatedLevel"
        "abbreviated level"                      | "marc:AbbreviatedLevel"
        "3"                                      | "marc:AbbreviatedLevel"
        "https://id.kb.se/marc/AbbreviatedLevel" | "marc:AbbreviatedLevel"
        "miniminivå"                             | "marc:AbbreviatedLevel"
        "monografisk resurs"                     | null // Ambiguous
        "monograph"                              | "Monograph"
        "seriell resurs"                         | "Serial"
        "topic"                                  | null
    }

    def "get ambiguous property mapping"() {
        expect:
        disambiguate.getAmbiguousPropertyMapping(alias) == mapped as Set

        where:
        alias           | mapped
        "identifikator" | ["identifier", "identifiedBy"]
        "anmärkning"    | ["note", "hasNote"]
        "ämne"          | [] // unambiguous
    }

    def "get ambiguous class mapping"() {
        expect:
        disambiguate.getAmbiguousClassMapping(alias) == mapped as Set

        where:
        alias   | mapped
        "karta" | ["Map", "Cartography"]
        "text"  | [] // unambiguous
    }

    def "get ambiguous enum mapping"() {
        expect:
        disambiguate.getAmbiguousEnumMapping(alias) == mapped as Set

        where:
        alias                | mapped
        "minimal level"      | ["marc:AbbreviatedLevel", "marc:MinimalLevel"]
        "monografisk resurs" | ["Monograph", "marc:SinglePartItemHolding"]
        "miniminivå"         | [] // unambiguous
    }

    def "expandPrefixed"() {
        expect:
        Disambiguate.expandPrefixed(s) == result

        where:
        s               | result
        "sao:Fysik"     | "https://id.kb.se/term/sao/Fysik"
        "unknown:Fysik" | "unknown:Fysik"
        "unprefixed"    | "unprefixed"
    }

    def "get domain of property"() {
        expect:
        disambiguate.getDomain(property) == domain

        where:
        property       | domain
        "production"   | "Instance"
        "contentType"  | "Work"
        "bibliography" | "Record"
        "subject"      | Disambiguate.UNKNOWN_DOMAIN
        "publisher"    | "Instance"
        "isbn"         | "Instance"
        "author"       | "Work"
    }

    def "get domain category"() {
        expect:
        disambiguate.getDomainCategory(domain) == category

        where:
        domain           | category
        "Print"          | Disambiguate.DomainCategory.INSTANCE
        "Embodiment"     | Disambiguate.DomainCategory.EMBODIMENT
        "Person"         | Disambiguate.DomainCategory.OTHER
        "Text"           | Disambiguate.DomainCategory.WORK
        "Creation"       | Disambiguate.DomainCategory.CREATION_SUPER
        "Record"         | Disambiguate.DomainCategory.ADMIN_METADATA
        "Unknown domain" | Disambiguate.DomainCategory.UNKNOWN
    }

    def "expand propertyChainAxiom"() {
        given:
        def p = new Path(path)
        disambiguate.expandChainAxiom(p)

        expect:
        p.stem == stem

        where:
        path                   | stem
        ['hasTitle']           | ['hasTitle']
        ['title']              | ['hasTitle', 'mainTitle']
        ['meta', 'changeNote'] | ['meta', 'hasChangeNote', 'label']
    }

    def "expand complex propertyChainAxiom"() {
        given:
        def p = new Path(before)
        disambiguate.expandChainAxiom(p)

        expect:
        p.stem == stem && p.branches as Set == branches

        where:
        before     | stem             | branches
        ['isbn']   | ['identifiedBy'] | [new Path.Branch(['value']), new Path.Branch(['rdf:type'], new QueryTree.VocabTerm("ISBN"))] as Set
        ['author'] | ['contribution'] | [new Path.Branch(['agent']), new Path.Branch(['role'], new QueryTree.Link("https://id.kb.se/relator/author"))] as Set
    }
}
