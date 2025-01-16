package whelk.search2.parse

import spock.lang.Specification
import whelk.exception.InvalidQueryException
import whelk.search2.parse.Lex
import whelk.search2.parse.Parse

class ParseSpec extends Specification {

    def "normal parse"() {
        given:
        def input = "AAA BBB AND (CCC OR DDD)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "implicit and group"() {
        given:
        def input = "AAA BBB (CCC OR DDD)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "parse negative"() {
        given:
        def input = "!AAA"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "parse negative2"() {
        given:
        def input = "NOT AAA"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "parse negative3"() {
        given:
        def input = "NOT (AAA)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "crazy grouping"() {
        given:
        def input = "AAA BBB AND (CCC OR DDD OR (EEE) AND (FFF OR GGG))"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "fail crazy grouping with bad parens"() {
        given:
        def input = "AAA BBB AND (CCC OR DDD OR (EEE) AND (FFF OR GGG)"
        def lexedSymbols = Lex.lexQuery(input)

        when:
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        then:
        thrown InvalidQueryException
    }

    def "super basic parse"() {
        given:
        def input = "AAA"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "super basic parse2"() {
        given:
        def input = "AAA BBB"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "basic code"() {
        given:
        def input = "förf:AAA"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "basic code2"() {
        given:
        def input = "förf=AAA"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "quoted code"() {
        given:
        def input = "förf:\"AAA\""
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "quoted code2"() {
        given:
        def input = "förf:\"förf:\""
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "quoted code3"() {
        given:
        def input = "\"förf:\":\"AAA\""
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "quoted code4"() {
        given:
        def input = "\"Kod: författare och annat:\":(\"AAA\" OR BBB)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "code group"() {
        given:
        def input = "förf:(AAA)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "code group2"() {
        given:
        def input = "förf:(AAA OR BBB AND CCC)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "Bad use of code"() {
        given:
        def input = "förf:"
        def lexedSymbols = Lex.lexQuery(input)

        when:
        Parse.parseQuery(lexedSymbols)
        then:
        thrown InvalidQueryException
    }

    def "Bad use of code2"() {
        given:
        def input = "AAA OR förf:"
        def lexedSymbols = Lex.lexQuery(input)

        when:
        Parse.parseQuery(lexedSymbols)
        then:
        thrown InvalidQueryException
    }

    def "Don't parse missing or-tail"() {
        given:
        def input = "AAA BBB AND (CCC OR)"
        def lexedSymbols = Lex.lexQuery(input)

        when:
        Parse.parseQuery(lexedSymbols)
        then:
        thrown InvalidQueryException
    }

    def "Don't parse missing and-tail"() {
        given:
        def input = "AAA BBB AND"
        def lexedSymbols = Lex.lexQuery(input)

        when:
        Parse.parseQuery(lexedSymbols)
        then:
        thrown InvalidQueryException
    }

    def "code binop"() {
        given:
        def input = "published: 2022"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "code binop2"() {
        given:
        def input = "published:2022"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "code binop3"() {
        given:
        def input = "published=2022"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "code binop4"() {
        given:
        def input = "published<2022"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "code binop5"() {
        given:
        def input = "published>=2022"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "Fail compare with group"() {
        given:
        def input = "AAA < (CCC)"
        def lexedSymbols = Lex.lexQuery(input)

        when:
        Parse.parseQuery(lexedSymbols)
        then:
        thrown InvalidQueryException
    }

    def "Fail compare with not"() {
        given:
        def input = "AAA < ! CCC"
        def lexedSymbols = Lex.lexQuery(input)

        when:
        Parse.parseQuery(lexedSymbols)
        then:
        thrown InvalidQueryException
    }

    def "Fail compare with not2"() {
        given:
        def input = "AAA < NOT CCC"
        def lexedSymbols = Lex.lexQuery(input)

        when:
        Parse.parseQuery(lexedSymbols)
        then:
        thrown InvalidQueryException
    }

    def "Fail compare with like"() {
        given:
        def input = "AAA < ~ CCC"
        def lexedSymbols = Lex.lexQuery(input)

        when:
        Parse.parseQuery(lexedSymbols)
        then:
        thrown InvalidQueryException
    }

}