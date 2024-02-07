package whelk.xlql

import spock.lang.Specification
import whelk.Whelk

class DisambiguateSpec extends Specification {
    private static Whelk whelk = Whelk.createLoadedSearchWhelk();
    private static Disambiguate disambiguate = new Disambiguate(whelk);

    def "map property alias to kbv term"() {
        expect:
        disambiguate.mapToKbvProperty(alias) == property

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
        "isbn"         | "Resource"
    }

    def "get domain group"() {
        expect:
        disambiguate.getDomainGroup(domain) == group

        where:
        domain       | group
        "Print"      | Disambiguate.DomainGroup.INSTANCE
        "Embodiment" | Disambiguate.DomainGroup.INSTANCE_SUPER
        "Person"     | Disambiguate.DomainGroup.OTHER
        "Text"       | Disambiguate.DomainGroup.WORK
        "Record"     | Disambiguate.DomainGroup.ADMIN_METADATA
    }

    def "expand propertyChainAxiom"() {
        expect:
        disambiguate.expandChainAxiom(path) == expanded

        where:
        path                   | expanded
        ['hasTitle']           | ['hasTitle'] // Nothing to expand
        ['title']              | ['hasTitle', 'mainTitle']
        ['meta', 'changeNote'] | ['meta', 'hasChangeNote', 'label']
        ['isbn']               | ['identifiedBy', 'value']
    }
}
