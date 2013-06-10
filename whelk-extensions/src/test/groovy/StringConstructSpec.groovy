package se.kb.libris.whelks.plugin

import spock.lang.Specification

class StringConstructSpec extends Specification {

    def "should construct person labels"() {
        given:
        def nt = new StringConstruct(
                [", ": ["familyName",
                [" ": ["givenName", ["(%s)": [1, "numeration"]]]],
                "personTitle",
                ["%s-%s": [1, "birthYear", "deathYear"]]
                ]])

        expect:
        nt.expand(data) == label

        where:
        data                                        | label
        [familyName: "Jansson", givenName: "Tove", numeration: "VI", personTitle: "Esq.",
         birthYear: "1914", deathYear: "2001"]      | "Jansson, Tove (VI), Esq., 1914-2001"

        [familyName: "Jansson", givenName: "Tove", numeration: "VI", birthYear: "1914",
         deathYear: "2001", ]                       | "Jansson, Tove (VI), 1914-2001"

        [familyName: "Jansson", givenName: "Tove", personTitle: "Esq.",
         birthYear: "1914", deathYear: "2001"]      | "Jansson, Tove, Esq., 1914-2001"

        [familyName: "Jansson", givenName: "Tove", birthYear: "1914",
         deathYear: "2001"]                         | "Jansson, Tove, 1914-2001"

        [familyName: "Jansson", givenName: "Tove",
         birthYear: "1914"]                         | "Jansson, Tove, 1914-"

        [givenName: "Tove", birthYear: "1914"]      | "Tove, 1914-"

        [familyName: "Jansson", birthYear: "1914"]  | "Jansson, 1914-"

        [familyName: "Jansson", deathYear: "2001"]  | "Jansson, -2001"

        [familyName: "Jansson", givenName: "Tove"]  | "Jansson, Tove"

    }

}
