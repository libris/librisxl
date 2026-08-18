package whelk.sru.cql

import org.antlr.v4.runtime.misc.ParseCancellationException
import spock.lang.Specification

class CqlToXlSpec extends Specification {

    def "simple query"() {
        given:
        String cqlQuery = 'AAA'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '(aaa)'
    }

    def "CQL has left-to-right precedence"() {
        given:
        String cqlQuery = 'A OR B AND C'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '(((a) OR (b)) AND (c))'
    }

    def "CQL has left-to-right precedence2"() {
        given:
        String cqlQuery = 'A AND B OR C'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '(((a) AND (b)) OR (c))'
    }

    def "Group bonanza"() {
        given:
        String cqlQuery = 'A AND (B OR (C)) AND ((D))'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '(((a) AND ((b) OR (c))) AND (d))'
    }

    def "any & all"() {
        given:
        String cqlQuery = 'field1 any "a b c" or field2 all "c d e"'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '("field1"=((a) OR (b) OR (c)) OR "field2"=((c) AND (d) AND (e)))'
    }

    def "example query 1"() {
        given:
        String cqlQuery = 'dc.title any fish or (dc.creator any sanderson and dc.identifier = "id:1234567")'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '("dc.title"=(fish) OR ("dc.creator"=(sanderson) AND "dc.identifier"=(id 1234567)))'
    }

    def "example query 2, ignore prefix assignments"() {
        given:
        String cqlQuery = '> dc = "info:srw/context-sets/1/dc-v1.1" dc.title any fish'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '"dc.title"=(fish)'
    }

    def "example query 3, ignore relation modifiers"() {
        given:
        String cqlQuery = 'dc.title any/rel.algorithm=cori fish'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '"dc.title"=(fish)'
    }

    def "example query 4, prox is and"() {
        given:
        String cqlQuery = 'dc.title any fish prox/unit=word/distance>3 dc.title any squirrel'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '("dc.title"=(fish) AND "dc.title"=(squirrel))'
    }

    def "example query 5, ignore sort"() {
        given:
        String cqlQuery = '"dinosaur" sortBy dc.date/sort.descending dc.title/sort.ascending'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '(dinosaur)'
    }

    def "hyphens"() {
        given:
        String cqlQuery = '978-1-0732-3504-9'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '(978-1-0732-3504-9)'
    }

    def "special char"() {
        given:
        String cqlQuery = 'nånting:'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '(nånting)'
    }

    def "+"() {
        given:
        String cqlQuery = 'bath.isbn= ECHO and +MAIN'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '("bath.isbn"=(echo) AND (+main))'
    }

    def "xlql keywords"() {
        given:
        String cqlQuery = '"A ELLER B OCH C"'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '(a eller b och c)'
    }

    def "':' in qutoed string"() {
        given:
        String cqlQuery = '"Title:subtitle"'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '(title subtitle)'
    }

    def "':' in qutoed string"() {
        given:
        String cqlQuery = '((z3950.1003 = "Lidbeck, Lasse") and (z3950.7 = "9129652634*")) and (z3950.4 = "Kungar och drottningar i Sverige :") sortBy libris.rank'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '(("z3950.1003"=(lidbeck, lasse) AND "z3950.7"=(9129652634*)) AND "z3950.4"=(kungar och drottningar i sverige))'
    }

    //

    def "tutorial example"() {
        given:
        String cqlQuery = '(>x="http://www.loc.gov/srw/index-sets/dc" x.title=dinosaur) and (>aVerySillyLongPrefix="http://www.loc.gov/srw/index-sets/dc" aVerySillyLongPrefix.author=farlow)'

        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        expect:
        translatedXlQuery == '("x.title"=(dinosaur) AND "aVerySillyLongPrefix.author"=(farlow))'
    }

    def "failed parse"() {
        given:
        String cqlQuery = '('

        when:
        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        then:
        thrown ParseCancellationException
    }

    def "failed lex"() {
        given:
        String cqlQuery = '😇'

        when:
        String translatedXlQuery = Translation.translateCqlToXlQuery(cqlQuery)

        then:
        thrown ParseCancellationException
    }

}
