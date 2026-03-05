package whelk.sru.cql

import spock.lang.Specification
import whelk.exception.InvalidQueryException
import whelk.sru.cql.Lex
import whelk.sru.cql.Parse

class ParseSpec extends Specification {

    def "normal parse"() {
        given:
        //def input = "/relevant=hej"
        def input = "hej hej hej hej"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.CqlQuery parseTree = Parse.parseQuery(lexedSymbols)

        expect:
        parseTree == new Parse.CqlQuery(

        )
    }
}