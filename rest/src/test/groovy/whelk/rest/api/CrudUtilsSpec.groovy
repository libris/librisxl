package whelk.rest.api

import spock.lang.Specification
import whelk.rest.api.CrudUtils

import javax.servlet.http.HttpServletRequest

class CrudUtilsSpec extends Specification {
    HttpServletRequest request

    void setup() {
        request = GroovyMock(HttpServletRequest.class)
    }

    def "Should return correct content type for request"() {
        given:
        request.getHeader(_) >> {
            "application/json, */*, text/html"
        }
        request.getRequestURI() >> {
          "/"
        }
        when:
        String contentType = CrudUtils.getBestContentType(request)
        then:
        contentType == "application/json"

    }

    def "Should return correct content type"() {
        expect:
        CrudUtils.getBestContentType(mimeType, suffix) == contentType
        where:
        mimeType                                            | suffix   | contentType
        "text/turtle"                                       | null     | "text/turtle"
        "application/json"                                  | null     | "application/json"
        "application/ld+json"                               | null     | "application/ld+json"
        "*/*"                                               | null     | "application/ld+json"
        "*/*"                                               | "jsonld" | "application/ld+json"
        "application/ld+json"                               | "jsonld" | "application/ld+json"
        "application/json"                                  | "jsonld" | "application/json"
        "*/*"                                               | "json"   | "application/json"
        "application/ld+json"                               | "json"   | "application/ld+json"
        "*/*"                                               | "ttl"    | "text/turtle"
        "*/*"                                               | "rdf"    | "application/rdf+xml"
        "application/x-octet-stream"                        | null     | null
        "text/turtle;q=0.8"                                 | null     | "text/turtle"
        "text/turtle, application/json;q=0.8"               | null     | "text/turtle"
        "text/turtle;q=0.1, application/json;q=0.8"         | null     | "application/json"
        "*/*, text/turtle;q=0.1, application/json;q=0.8"    | null     | "application/ld+json"
        "text/turtle, application/signed-exchange;v=b3"     | null     | "text/turtle"
    }

    def "Should convert suffix to MIME type"() {
        expect:
        CrudUtils.getMimeTypeForSuffix(suffix) == mimeType
        where:
        suffix   | mimeType
        "jsonld" | "application/ld+json"
        "json"   | "application/json"
        "rdf"    | "application/rdf+xml"
        "ttl"    | "text/turtle"
    }

    def "Should throw on invalid suffix"() {
        when:
        CrudUtils.getMimeTypeForSuffix('html')

        then:
        thrown(Crud.NotFoundException)
    }

    def "Should find matching MIME type"() {
        expect:
        CrudUtils.findMatchingMimeType(sought, available) == result
        where:
        sought      | available                              | result
        "*/*"       | ["application/ld+json"]                | "application/ld+json"
        "text/html" | ["application/ld+json"]                | null
        "text/*"    | ["application/ld+json", "text/turtle"] | "text/turtle"
    }

    def "Should get match wildcard MIME type"() {
        expect:
        CrudUtils.isMatch(myMimeType, yourMimeType) == result
        where:
        myMimeType            | yourMimeType  | result
        "text/html"           | "text/html"   | true
        "text/turtle"         | "text/html"   | false
        "text/*"              | "text/*"      | true
        "text/turtle"         | "text/*"      | true
        "text/*"              | "text/turtle" | true
        "application/ld+json" | "text/*"      | false
        "application/ld+json" | "*/*"         | true
        "bad_input"           | "*/*"         | false
        "*/*"                 | "bad_input"   | false
        null                  | "*/*"         | false
        "*/*"                 | null          | false
    }

    def "Should pick best matching MIME type"() {
        expect:
        assert CrudUtils.getBestMatchingMimeType(allowed, desired) == expected
        where:
        allowed                           | desired                              | expected
        ["application/json", "text/html"] | ["application/rdf+xml", "text/html"] | "text/html"
        ["application/json", "text/html"] | ["*/*"]                              | "application/ld+json"
        ["application/json"]              | ["text/html"]                        | null
    }

    def "Should sort MIME types by quality"() {
        expect:
        assert CrudUtils.parseAcceptHeader(mimeTypes).collect{ it.toString() } == sortedMimeTypes
        where:
        mimeTypes                                                | sortedMimeTypes
        "*/*, text/html;q=0.1, application/json;q=0.8"           | ["*/*", "application/json", "text/html"]
        ''                                                       | []
        "text/html;q=0.1"                                        | ["text/html"]
        "application/json, */*;q=0.1, text/html;q=0.1"           | ["application/json", "text/html", "*/*"]
        "application/json ,  text/html;q=0.1 , */*;q=0.2"        | ["application/json", "*/*", "text/html"]
        "x/x;p=x;q=1.0;extension-param=ignored"                  | ['x/x; p=x']
        '*/*, text/*, text/rdf, text/rdf;p=x'                    | ['text/rdf; p=x', 'text/rdf', 'text/*', '*/*']
        'application/ld+json, application/ld+json;profile="a b"' | ['application/ld+json; profile="a b"', 'application/ld+json']
    }

    def "Should throw on invalid Accept header"() {
        when:
        CrudUtils.parseAcceptHeader("application/ld+json;q=one")

        then:
        thrown(BadRequestException)
    }

    def "Should throw on invalid Accept header II"() {
        when:
        CrudUtils.parseAcceptHeader("ld+json")

        then:
        thrown(BadRequestException)
    }

}
