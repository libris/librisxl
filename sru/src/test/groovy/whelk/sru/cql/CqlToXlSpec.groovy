package whelk.sru.cql

import spock.lang.Specification

class CqlToXlSpec extends Specification {
def "simple query"() {
    given:
    //String input = "AAA AND BBB"
    //String input = "dc.title any fish or/rel.combine=sum dc.creator any sanderson"
    //String input = "AAA or (BBB and CCC)"
    //String input = "A OR B AND C"
    //String input = "A OR (B AND C)"
    //String input = "author=(kern or ritchie)"; // PROBLEM!
    //String input = "author all \"^kernighan ritchie\""
    String input = "author any \"^kernighan ritchie knuth\""
    /*String input = "dc.author=(kern* or ritchie) and\n" +
            "\t(bath.title exact \"the c programming language\" or\n" +
            "\t dc.title=elements prox///4 dc.title=programming) and\n" +
            "\tsubject any/relevant \"style design analysis\""*/
    //String input = "author all \"^kernighan ritchie\""
    String output = Translation.translateCqlToXlQuery(input)

    expect:
    output == "good output"
    }
}
