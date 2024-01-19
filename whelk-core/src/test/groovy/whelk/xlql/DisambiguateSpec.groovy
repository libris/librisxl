package whelk.xlql

import spock.lang.Specification
import whelk.Whelk

class DisambiguateSpec extends Specification {
    private static Whelk whelk = Whelk.createLoadedSearchWhelk();
    private static Disambiguate disambiguator = new Disambiguate(whelk);

    def "map property alias to kbv term"() {
        expect:
        disambiguator.mapToKbvProperty(alias) == property

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
}
