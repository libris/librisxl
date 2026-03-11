package whelk.sru

import spock.lang.Specification
import sru.whelk.cqlLexer
import sru.whelk.cqlParser

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.CharStreams;

class CqlToXlSpec extends Specification {
def "simple query"() {
    given:

    String input = "AAA AND BBB"

    cqlLexer lexer = new cqlLexer( CharStreams.fromString(input) )
    CommonTokenStream tokens = new CommonTokenStream(lexer)
    cqlParser parser = new cqlParser(tokens)
    cqlParser.CqlContext cst = parser.cql()
    String output = null

    expect:
    output == "good output"
    }
}
