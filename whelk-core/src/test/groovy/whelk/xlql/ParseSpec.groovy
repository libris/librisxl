package whelk.xlql

import spock.lang.Specification
import whelk.xlql.Parse.ParseException

class ParseSpec extends Specification {

    def "normal parse"() {
        given:
        def input = "AAA BBB and (CCC or DDD)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "crazy grouping"() {
        given:
        def input = "AAA BBB and (CCC or DDD or (EEE) AND (FFF OR GGG))"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree != null
    }

    def "fail crazy grouping with bad parens"() {
        given:
        def input = "AAA BBB and (CCC or DDD or (EEE) AND (FFF OR GGG)"
        def lexedSymbols = Lex.lexQuery(input)

        when:
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        then:
        thrown ParseException
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
        def input = "förf:(AAA or BBB and CCC)"
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
        thrown ParseException
    }

    def "Bad use of code2"() {
        given:
        def input = "AAA or förf:"
        def lexedSymbols = Lex.lexQuery(input)

        when:
        Parse.parseQuery(lexedSymbols)
        then:
        thrown ParseException
    }

    def "Don't parse missing or-tail"() {
        given:
        def input = "AAA BBB and (CCC or)"
        def lexedSymbols = Lex.lexQuery(input)

        when:
        Parse.parseQuery(lexedSymbols)
        then:
        thrown ParseException
    }

    def "Don't parse missing and-tail"() {
        given:
        def input = "AAA BBB and"
        def lexedSymbols = Lex.lexQuery(input)

        when:
        Parse.parseQuery(lexedSymbols)
        then:
        thrown ParseException
    }
}