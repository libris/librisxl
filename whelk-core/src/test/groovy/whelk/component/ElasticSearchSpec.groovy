package whelk.component

import spock.lang.Specification

class ElasticSearchSpec extends Specification {
    def "subMap ordering"() {
        // getShapeForIndex relies on this semi-undocumented behavior of subMap
        expect:
        ['a': 1, 'b': 2].subMap(['b', 'a']).values().asList() == [2, 1]
        ['a': 1, 'b': 2].subMap(['a', 'b']).values().asList() == [1, 2]
    }
}
