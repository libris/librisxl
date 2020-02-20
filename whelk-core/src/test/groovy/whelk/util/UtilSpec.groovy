package whelk.util

import spock.lang.Specification

class UtilSpec extends Specification {
    def "lazyIterableChain"() {
        given:
        def iterables = [[1,2,3], [4,5,6], [7,8,9]].collect{ return { -> it } as Closure }

        expect:
        Util.lazyIterableChain(iterables).collect() == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    }
}
