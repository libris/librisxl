package whelk.rest.api

import com.google.common.net.MediaType
import org.apache.jena.shared.NotFoundException
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
        given:
        request.getHeader(_) >> {
            accept
        }
        request.getRequestURI() >> {
            "/id${suffix}"
        }
        expect:
        CrudUtils.getBestContentType(request) == contentType
        where:
        accept                                              | suffix    || contentType
        "text/turtle"                                       | ''        || "application/ld+json"
        "application/json"                                  | ''        || "application/json"
        "application/ld+json"                               | ''        || "application/ld+json"
        "*/*"                                               | ''        || "application/ld+json"
        "*/*"                                               | ".jsonld" || "application/ld+json"
        "application/ld+json"                               | ".jsonld" || "application/ld+json"
        "application/json"                                  | ".jsonld" || "application/ld+json"
        "*/*"                                               | ".json"   || "application/json"
        "application/ld+json"                               | ".json"   || "application/json"
        "application/x-octet-stream"                        | ''        || "application/ld+json"
        "text/turtle;q=0.8"                                 | ''        || "application/ld+json"
        "text/turtle, application/json;q=0.8"               | ''        || "application/json"
        "text/turtle;q=0.1, application/json;q=0.8"         | ''        || "application/json"
        "*/*, text/turtle;q=0.1, application/json;q=0.8"    | ''        || "application/ld+json"
        "text/turtle, application/signed-exchange;v=b3"     | ''        || "application/ld+json"
    }

    def "Should throw on invalid suffix"() {
        given:
        request.getHeader(_) >> {
            '*/*'
        }
        request.getRequestURI() >> {
            "/id${suffix}"
        }
        when:
        CrudUtils.getBestContentType(request)

        then:
        thrown(Crud.NotFoundException)

        where:
        suffix << ['.rdf', '.ttl', '.invalid']
    }

    static final def m = MediaType.&parse
    def "Should pick best matching MIME type"() {
        expect:
        assert CrudUtils.getBestMatchingMimeType(allowed, desired) == expected
        where:
        allowed                           | desired                              | expected
        [m("application/json"), m("text/html")] | [m("application/rdf+xml"), m("text/html")] | m("text/html")
        [m("application/json"), m("text/html")] | [m("*/*")]                                 | m("application/json")
        [m("application/json"), m("text/html")] | [m("text/*")]                              | m("text/html")
        [m("application/json")]                 | [m("text/html")]                           | m("application/json")
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

        where:
        header << ["application/ld+json;q=one", "ld+json"]
    }
}
