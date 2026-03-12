package whelk.sru.cql

import spock.lang.Specification

class CqlToXlSpec extends Specification {

    def "simple query"() {
        given:
        String cqlQuery = 'AAA'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '(AAA) AND type=Instance'
    }

    def "CQL has left-to-right precedence"() {
        given:
        String cqlQuery = 'A OR B AND C'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '((A OR B) AND C) AND type=Instance'
    }

    def "CQL has left-to-right precedence2"() {
        given:
        String cqlQuery = 'A AND B OR C'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '((A AND B) OR C) AND type=Instance'
    }

    def "Group bonanza"() {
        given:
        String cqlQuery = 'A AND (B OR (C)) AND ((D))'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '((A AND (B OR C)) AND D) AND type=Instance'
    }

    def "any & all"() {
        given:
        String cqlQuery = 'field1 any "a b c" or field2 all "c d e"'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '("field1"=(a OR b OR c) OR "field2"=(c AND d AND e)) AND type=Instance'
    }

    def "example query 1"() {
        given:
        String cqlQuery = 'dc.title any fish or (dc.creator any sanderson and dc.identifier = "id:1234567")'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '("dc.title"=fish OR ("dc.creator"=sanderson AND "dc.identifier"=id:1234567)) AND type=Instance'
    }

    def "example query 2, ignore prefix assignments"() {
        given:
        String cqlQuery = '> dc = "info:srw/context-sets/1/dc-v1.1" dc.title any fish'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '("dc.title"=fish) AND type=Instance'
    }

    def "example query 3, ignore relation modifiers"() {
        given:
        String cqlQuery = 'dc.title any/rel.algorithm=cori fish'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '("dc.title"=fish) AND type=Instance'
    }

    def "example query 4, prox is and"() {
        given:
        String cqlQuery = 'dc.title any fish prox/unit=word/distance>3 dc.title any squirrel'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '("dc.title"=fish AND "dc.title"=squirrel) AND type=Instance'
    }

    def "example query 5, ignore sort"() {
        given:
        String cqlQuery = '"dinosaur" sortBy dc.date/sort.descending dc.title/sort.ascending'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '(dinosaur) AND type=Instance'
    }

    def "tutorial example"() {
        given:
        String cqlQuery = '(>x="http://www.loc.gov/srw/index-sets/dc" x.title=dinosaur) and (>aVerySillyLongPrefix="http://www.loc.gov/srw/index-sets/dc" aVerySillyLongPrefix.author=farlow)'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '("x.title"=dinosaur AND "aVerySillyLongPrefix.author"=farlow) AND type=Instance'
    }

}
