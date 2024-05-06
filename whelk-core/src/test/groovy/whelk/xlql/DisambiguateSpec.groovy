package whelk.xlql

import spock.lang.Ignore
import spock.lang.Specification
import whelk.Whelk

@Ignore
class DisambiguateSpec extends Specification {
    private static Whelk whelk = Whelk.createLoadedSearchWhelk();
    private static Disambiguate disambiguate = new Disambiguate(whelk);

    def "map property alias to kbv term"() {
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
        "jurisdiction"                   | null
        "type"                           | "rdf:type"
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
        expect:
        disambiguate.expandChainAxiom(path).path() == extendedPath

        where:
        path                   | extendedPath
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
        ['isbn']   | ['identifiedBy'] | [new Path.Branch(['value']), new Path.Branch(['rdf:type'], new SimpleQueryTree.VocabTerm("ISBN"))] as Set
        ['author'] | ['contribution'] | [new Path.Branch(['agent']), new Path.Branch(['role'], new SimpleQueryTree.Link("https://id.kb.se/relator/author"))] as Set
    }
}
