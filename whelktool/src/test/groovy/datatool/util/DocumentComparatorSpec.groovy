package datatool.util

import org.codehaus.jackson.map.ObjectMapper
import spock.lang.Shared
import spock.lang.Specification

class DocumentComparatorSpec extends Specification {
    @Shared
    ObjectMapper mapper = new ObjectMapper()

    def "isEqual"() {
        given:
        DocumentComparator d = new DocumentComparator({ o -> ("ordered" == o) })

        expect:
        d.isEqual(a, b) == eq
        d.isEqual(b, a) == eq

        where:
        a                    | b                    || eq
        [:]                  | [:]                  || true
        [1: 1]               | [1: 1]               || true
        [:]                  | [1: 1]               || false
        [1: 1]               | [:]                  || false
        [1: [2: 2]]          | [1: [2: 2]]          || true
        [1: [2: 2]]          | [1: 1]               || false
        [0: [1, 2, 3, 4]]    | [0: [4, 2, 1, 3]]    || true

        [1: [2: ["a", "b"]]] | [1: [2: ["b", "a"]]] || true
        ["a": [1, 2, 3]]     | ["a": [2, 3, 1]]     || true

        ["ordered": [1, 2]]  | ["ordered": [1, 2]]  || true
        ["ordered": [1, 2]]  | ["ordered": [2, 1]]  || false

        // one element list equals element
        ["x": ["a"]]         | ["x": "a"]           || true
        ["x": [["n": 2]]]    | ["x": ["n": 2]]      || true
    }

    def "isSubset"() {
        given:
        DocumentComparator d = new DocumentComparator({ o -> ("ordered" == o) })

        expect:
        d.isSubset(a, b) == eq

        where:
        a                                           | b                                                               || eq
        [:]                                         | [:]                                                             || true
        [1: 1]                                      | [1: 1]                                                          || true
        [1: 1, 2: 2]                                | [1: 1]                                                          || false
        [1: 2]                                      | [1: 1]                                                          || false
        [:]                                         | [1: [2: 2]]                                                     || true
        [1: [:]]                                    | [1: [2: 2]]                                                     || true
        ["x": ["b", "c"]]                           | ["x": ["a", "b", "c"]]                                          || true
        ["x": ["c", "a"]]                           | ["x": ["a", "b", "c"]]                                          || true
        ["x": ["a", "a"]]                           | ["x": ["a"]]                                                    || false
        ["x": ["a", "a", "a"]]                      | ["x": ["a", "a", "a"]]                                          || true
        ["x": ["a", "a", "a", "a", "a"]]            | ["x": ["a", "a", "a", "a"]]                                     || false

        ["x": ["a", ["n": 2]], "y": [2: 2]]         | ["x": ["a", "b", ["m": 1, "n": 2]], "y": [1: 1, 2: 2], "z": []] || true
        ["x": ["a", ["n": 2, "Q": 3]], "y": [2: 2]] | ["x": ["a", "b", ["m": 1, "n": 2]], "y": [1: 1, 2: 2], "z": []] || false

        ["x": ["ordered": [1, 3, 5, 6, 8, 9]]]      | ["x": ["ordered": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]]             || true
        ["x": ["ordered": [1, 3, 6, 5, 8, 9]]]      | ["x": ["ordered": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]]             || false
    }

    def "handles nested unordered lists"() {
        given:
        def a = ["x": [[[5, 6, 7, 8], [1, 2, '3', 4]], [[13, 14, 15, [16: 16]], ['9', 10, 11, 12]]]]
        def b = ["x": [[[10, '9', 12, 11], [15, 13, [16: 16], 14]], [[4, '3', 2, 1], [7, 8, 5, 6]]]]

        expect:
        new DocumentComparator().isEqual(a, b) == true
        new DocumentComparator().isSubset(a, b) == true
        new DocumentComparator().isSubset(a, b) == true
    }

    def "document should equal itself"() {
        given:
        Map a = loadTestData('work1.json')

        expect:
        new DocumentComparator().isEqual(a, a) == true
        new DocumentComparator().isSubset(a, a) == true
        new DocumentComparator().isSubset(a, a) == true
    }

    def "order of elements should not matter"() {
        given:
        Map a = loadTestData('work1.json')
        Map b = loadTestData('work1-reordered.json')

        expect:
        new DocumentComparator().isEqual(a, b) == true
        new DocumentComparator().isSubset(a, b) == true
        new DocumentComparator().isSubset(b, a) == true
    }

    def "subset"() {
        given:
        Map a = loadTestData('work1.json')
        Map b = loadTestData('work1-subset.json')
        DocumentComparator diff = new DocumentComparator()

        expect:
        diff.isEqual(a, b) == false
        diff.isSubset(a, b) == false
        diff.isSubset(b, a) == true
    }

    def "not equal"() {
        given:
        Map a = loadTestData('work1.json')
        Map b = loadTestData('work1-changed.json')
        DocumentComparator diff = new DocumentComparator()

        expect:
        diff.isEqual(a, b) == false
        diff.isSubset(a, b) == false
        diff.isSubset(b, a) == false
    }

    def "order matters for termComponentList"() {
        given:
        Map a = loadTestData('ordered.json')
        Map b = loadTestData('ordered-reordered.json')
        DocumentComparator diff = new DocumentComparator()

        expect:
        diff.isEqual(a, b) == false
        diff.isSubset(a, b) == false
        diff.isSubset(b, a) == false
    }

    def "isSubset finds solution"() {
        given:
        Map a = loadTestData('unordered-subset.json')
        Map b = loadTestData('unordered-superset.json')
        DocumentComparator diff = new DocumentComparator()

        expect:
        diff.isEqual(a, b) == false
        diff.isSubset(a, b) == true
        diff.isSubset(b, a) == false
    }

    private Map loadTestData(String file) {
        String json = this.getClass().getResource(file).text
        return mapper.readValue(json, Map.class)
    }
}
