package whelk.util

import spock.lang.Specification
import spock.lang.Unroll

class IrisSpec extends Specification {

    @Unroll
    def "usable IRI is not broken: #iri"() {
        expect:
        !Iris.isBroken(iri)

        where:
        iri << [
                "https://libris.kb.se/abc123",
                "https://id.kb.se/term/sao/Marsvin%20som%20s%C3%A4llskapsdjur",
                "https://ja.wikipedia.org/wiki/モルモット",
                "https://libris.kb.se/hold/123#it",
                "urn:isbn:0451450523",
                "mailto:foo@bar.com",
                // relative IRIs are not rejected
                "//foo",
                "relative/path",
        ]
    }

    @Unroll
    def "broken IRI is detected: #iri"() {
        expect:
        Iris.isBroken(iri)

        where:
        iri << [
                // IRIx.create() reports these as violations
                "http://foo:",
                "http://",
                "HTTP://EXAMPLE.COM/Path",
                "http://example.com:80/x",
                "http://example.com/%7euser",
                // ...and throws on these
                "not a uri at all",
                "https://example.com/ bar",
                "[sfsdfsdf]",
                "\"https://libris.kb.se/library/DIGI\"",
        ]
    }
}
