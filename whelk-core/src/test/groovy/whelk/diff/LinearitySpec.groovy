package whelk.diff

import spock.lang.Specification
import static whelk.util.Jackson.mapper

class LinearitySpec extends Specification {

    def "linearity 1"() {
        given:

        def versions = [
                [
                        "a":"b"
                ],
                [
                        "b":"c"
                ],
                [
                        "c":"d"
                ]
        ]

        // Common for all tests:
        def diffs = []
        for (int i = 0; i < versions.size()-1; ++i) {
            diffs.add( Diff.diff(versions[i], versions[i+1]) )
        }
        def recreatedVersions = [versions[0]]
        for (int i = 0; i < diffs.size(); ++i) {
            //System.err.println(mapper.writeValueAsString(diffs[i]))
            recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
        }
        expect:
        recreatedVersions.equals(versions)
    }

    def "linearity 2"() {
        given:

        def versions = [
                [
                        "a":["b", "c"]
                ],
                [
                        "a":["b", "c", "d"]
                ],
                [
                        "a":["b", "c", "d"], "e":["f"]
                ]
        ]

        // Common for all tests:
        def diffs = []
        for (int i = 0; i < versions.size()-1; ++i) {
            diffs.add( Diff.diff(versions[i], versions[i+1]) )
        }
        def recreatedVersions = [versions[0]]
        for (int i = 0; i < diffs.size(); ++i) {
            recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
        }
        expect:
        recreatedVersions.equals(versions)
    }

    def "linearity 3"() {
        given:

        def versions = [
                [
                        "a":["b", "c"]
                ],
                [
                        "a":["b", "c", "d"]
                ],
                [
                        "a":["b", "c"]
                ],
                [
                        "a":["b", "c", "d"], "e":["f"]
                ],
                [
                        "a":["b", "c"], "e": ["a": "b"]
                ]
        ]

        // Common for all tests:
        def diffs = []
        for (int i = 0; i < versions.size()-1; ++i) {
            diffs.add( Diff.diff(versions[i], versions[i+1]) )
        }
        def recreatedVersions = [versions[0]]
        for (int i = 0; i < diffs.size(); ++i) {
            recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
        }
        expect:
        recreatedVersions.equals(versions)
    }

    def "linearity 4"() {
        given:

        def versions = [
                [
                        "a":["b", "c"]
                ],
                [
                        "a":["b", "c", "d"]
                ],
                [
                        "a":["b", "c"]
                ]
        ]

        // Common for all tests:
        def diffs = []
        for (int i = 0; i < versions.size()-1; ++i) {
            diffs.add( Diff.diff(versions[i], versions[i+1]) )
        }
        def recreatedVersions = [versions[0]]
        for (int i = 0; i < diffs.size(); ++i) {
            recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
        }
        expect:
        recreatedVersions.equals(versions)
    }

    def "linearity 5"() {
        given:

        def versions = [
                [
                        "a":["b", "c"]
                ],
                [
                        "a":["b", "c", "d", ["a": ["b", 2]]]
                ],
                [
                        "a":["b", "c", "d", ["a": ["b", 3], "c":[true, "ok"]]]
                ],
                [
                        "a":["b", "c", "d", ["c": ["b", 3], "a":[true, "ok"]]]
                ],
        ]

        // Common for all tests:
        def diffs = []
        for (int i = 0; i < versions.size()-1; ++i) {
            diffs.add( Diff.diff(versions[i], versions[i+1]) )
        }
        def recreatedVersions = [versions[0]]
        for (int i = 0; i < diffs.size(); ++i) {
            //System.err.println(mapper.writeValueAsString(diffs[i]))
            recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
        }
        expect:
        recreatedVersions.equals(versions)
    }

    def "linearity 5"() {
        given:

        def versions = [
                [
                        "a":["b", "c", "d", "e", "f", "g"]
                ],
                [
                        "a":["e", "c", "d", "b", "f", "g", "h"]
                ],
                [
                        "a":["i", "e", "g", "d", "b", "f", "c"]
                ],
        ]

        // Common for all tests:
        def diffs = []
        for (int i = 0; i < versions.size()-1; ++i) {
            diffs.add( Diff.diff(versions[i], versions[i+1]) )
        }
        def recreatedVersions = [versions[0]]
        for (int i = 0; i < diffs.size(); ++i) {
            //System.err.println(mapper.writeValueAsString(diffs[i]))
            recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
        }
        expect:
        recreatedVersions.equals(versions)
    }

}