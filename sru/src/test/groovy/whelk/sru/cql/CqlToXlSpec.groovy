package whelk.sru.cql

import spock.lang.Specification
import sru.whelk.cql.cqlLexer
import sru.whelk.cql.cqlParser

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.CharStreams;

class CqlToXlSpec extends Specification {
def "simple query"() {
    given:
    //String input = "AAA AND BBB"
    //String input = "dc.title any fish or/rel.combine=sum dc.creator any sanderson"
    String input = "AAA or (BBB and CCC)"
    String output = Translation.translateCqlToXlQuery(input)

    expect:
    output == "good output"
    }
}
