package whelk.component

import spock.lang.Specification
import whelk.Document
import whelk.Whelk

class ElasticSearchSpec extends Specification {
    def "subMap ordering"() {
        // getShapeForIndex relies on this semi-undocumented behavior of subMap
        expect:
        ['a': 1, 'b': 2].subMap(['b', 'a']).values().asList() == [2, 1]
        ['a': 1, 'b': 2].subMap(['a', 'b']).values().asList() == [1, 2]
    }

    def "test"() {
        given:
        Whelk whelk = Whelk.createLoadedSearchWhelk()
//        Document doc = whelk.getDocument("l4xzstkx18cnmhf")
//        Document doc = whelk.getDocument("8slqsjtl5hs0bts")
//        doc = doc.getVirtualRecordIds().collect {doc.getVirtualRecord(it) }.find()
        Document doc = whelk.getDocument("j19pfz0cg2xl1f2v")
        String shape1 = whelk.elastic.getShapeForIndex(doc, whelk)
        String shape2 = whelk.elastic.getShapeForIndex2(doc, whelk)

        expect:
        true
    }
}
