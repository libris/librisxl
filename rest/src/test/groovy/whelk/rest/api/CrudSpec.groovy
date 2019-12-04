package whelk.rest.api

import org.codehaus.jackson.map.ObjectMapper
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Unroll
import whelk.Document
import whelk.IdType
import whelk.JsonLd
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.exception.ModelValidationException
import whelk.rest.security.AccessControl
import whelk.util.LegacyIntegrationTools

import javax.servlet.ServletInputStream
import javax.servlet.ServletOutputStream
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import javax.servlet.http.HttpServletResponseWrapper

import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND
import static javax.servlet.http.HttpServletResponse.SC_OK
import static whelk.rest.api.MimeTypes.JSON
import static whelk.rest.api.MimeTypes.JSONLD

/**
 * Created by markus on 2015-10-16.
 */
class CrudSpec extends Specification {

    Crud crud
    Whelk whelk
    PostgreSQLComponent storage
    AccessControl accessControl
    HttpServletRequest request
    HttpServletResponse response

    static final URI BASE_URI = Document.BASE_URI
    private static final ObjectMapper mapper = new ObjectMapper()


    void setup() {
        request = GroovyMock(HttpServletRequest.class)
        request.getRequestURI() >> {
            request.getPathInfo()
        }

        CapturingServletOutputStream out = new CapturingServletOutputStream()
        response = new HttpServletResponseWrapper(GroovyMock(HttpServletResponse.class)) {
            int status = 0
            String contentType
            def headers = [:]

            ServletOutputStream getOutputStream() {
                return out
            }

            void sendError(int sc, String mess) {
                this.status = sc
            }

            void setHeader(String h, String v) {
                headers.put(h, v)
            }

            String getHeader(String h) {
                return headers.get(h)
            }

            String getResponseBody() {
                return out.asString()
            }
        }
        storage = GroovyMock(PostgreSQLComponent.class)
        // We want to pass through calls in some cases
        accessControl = GroovySpy(AccessControl.class)
        whelk = new Whelk()
        whelk.storage = storage
        whelk.contextData = ['@context': [
                'examplevocab': 'http://example.com',
                'some_term': 'some_value']]
        whelk.displayData = ['lensGroups': [
                'chips': [lenses: ['Instance' : ['showProperties': ['prop1', 'prop2']]]],
                'cards': [lenses: ['Instance' : ['showProperties': ['prop1', 'prop2', 'prop3']]]]
        ]]
        whelk.vocabData = ['@graph': []]
        whelk.jsonld = new JsonLd(whelk.contextData, whelk.displayData, whelk.vocabData)
        GroovySpy(LegacyIntegrationTools.class, global: true)
        crud = new Crud(whelk)
        crud.init()
        crud.accessControl = accessControl
    }


    /*
     * API tests
     *
     */

    def "GET to / should display system information"() {
        given:
        request.getPathInfo() >> {
            "/"
        }
        storage.loadSettings("system") >> {
          ["version": "1.0"]
        }
        when:
        crud.doGet(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_OK
        assert response.getContentType() == "application/json"
    }


    // Tests for 405 Method Not Allowed

    def "PUT to / should return 405 Method Not Allowed"() {
        given:
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "PUT"
        }
        when:
        crud.doPut(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_METHOD_NOT_ALLOWED
    }

    def "DELETE to / should return 405 Method Not Allowed"() {
        given:
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "DELETE"
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_METHOD_NOT_ALLOWED
    }

    def "POST to /<id> should return 405 Method Not Allowed"() {
        given:
        request.getPathInfo() >> {
            "/1234"
        }
        request.getMethod() >> {
            "POST"
        }
        when:
        crud.doPost(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_METHOD_NOT_ALLOWED
    }


    // Tests for read

    def "GET /<id> should display requested document if it exists"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getPathInfo() >> {
            '/' + id
        }
        request.getHeader("Accept") >> {
            "*/*"
        }
        storage.load(_, _) >> {
            new Document(["@graph": [["@id": id, "foo": "bar"]]])
        }
        storage.loadEmbellished(_, _) >> {
            new Document(["@graph": [["@id": id, "foo": "bar"]]])
        }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_OK
        response.getContentType() == "application/ld+json"
    }

    def "GET /<sameAs ID> should return 302 Found"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def altId = BASE_URI.resolve("/alt_1234").toString()
        request.pathInfo >> {
            '/' + id
        }
        request.getPathInfo() >> {
            "/${id}".toString()
        }
        request.getHeader("Accept") >> {
            "*/*"
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(altId) >> {
            return null
        }
        storage.loadDocumentByMainId(id) >> {
            new Document(['@graph': [['@id': id, 'sameAs': [['@id': altId]]]]])
        }
        storage.getMainId(_) >> {
            return id
        }
        storage.getIdType(_) >> {
            return IdType.RecordSameAsId
        }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_FOUND
    }

    def "GET /<id> should return 404 Not Found if document can't be found"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getPathInfo() >> {
            '/' + id
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_NOT_FOUND
    }

    def "GET /<alternate_id> if URI not found should return 404 Not Found"() {
        given:
        def altId = BASE_URI.resolve("/alt_1234").toString()
        request.getPathInfo() >> {
            '/' + altId
        }
        request.getHeader("Accept") >> {
            "*/*"
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_NOT_FOUND
    }

    def "GET /<id> should return 410 Gone if document is deleted"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getPathInfo() >> {
            '/' + id
        }
        request.getHeader("Accept") >> {
            "*/*"
        }
        storage.load(_, _) >> {
            Document doc = new Document(["@graph": [["@id": id, "foo": "bar"]]])
            doc.deleted = true
            return doc
        }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_GONE
    }

    @Ignore
    def "GET /<id>?version=1 should display requested document if it exists"() {
        given:
        def id = BASE_URI.resolve("/1234?version=1").toString()
        request.getPathInfo() >> {
            id
        }
        request.getHeader("Accept") >> {
            "*/*"
        }
        request.getParameter("version") >> {
            "1"
        }
        storage.load(_, "1") >> {
            new Document(["@graph": [["@id": id, "foo": "bar"]]])
        }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_OK
        response.getContentType() == "application/ld+json"
    }

    //TODO: this doesn't really test anything
    def "GET /<id>?version=1 should return 404 Not Found if document can't be found"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getPathInfo() >> {
            '/' + id
        }
        request.getQueryString() >> {
            return 'version=1'
        }
        storage.load(_, _, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_NOT_FOUND
    }

    def "GET /<id>/data should return raw representation"() {
        // We don't allow this for now
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getPathInfo() >> {
            "/${id}/data".toString()
        }
        request.getHeader("Accept") >> {
            "application/ld+json"
        }
        storage.load(_, _) >> {
            new Document(["@graph": [["@id": id, "foo": "bar"]]])
        }
        when:
        crud.doGet(request, response)
        then:
        // FIXME validate response body
        response.getStatus() == HttpServletResponse.SC_OK
        response.getContentType() == "application/ld+json"
    }

    def "GET /<id>/data.jsonld should display document in JSON-LD format"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getPathInfo() >> {
            "/${id}/data.jsonld".toString()
        }
        request.getHeader("Accept") >> {
            "*/*"
        }
        storage.load(_, _) >> {
            new Document(["@graph": [["@id": id, "foo": "bar"]]])
        }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_OK
        response.getContentType() == "application/ld+json"
    }

    // FIXME we should use another status code here
    def "GET /<id>/data.json should display document in JSON format"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getPathInfo() >> {
            "/${id}/data.json".toString()
        }
        request.getHeader("Accept") >> {
            "*/*"
        }
        storage.load(_, _) >> {
            new Document(["@graph": [
                    ["@id": id,
                     "foo": "bar",
                     "baz": ["@id": "examplevocab:"],
                     "quux": ["@id": "some_term"],
                     "bad_ref": ["@id": "invalid:ref"],
                     "mainEntity": ["@id": "main_id"]
                    ],
                    ["@id": "main_id"]
            ]])
        }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_OK
        response.getContentType() == "application/json"
    }

    def "GET /<id>/data.ttl should return 404 Not Found"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getPathInfo() >> {
            "/${id}/data.ttl".toString()
        }
        request.getHeader("Accept") >> {
            "*/*"
        }
        storage.load(_, _) >> {
            new Document(["@graph": [["@id": id, "foo": "bar"]]])
        }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus() == SC_NOT_FOUND
    }

    def "GET /<id>/data.rdf should return 404 Not Found"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getPathInfo() >> {
            "/${id}/data.rdf".toString()
        }
        request.getHeader("Accept") >> {
            "*/*"
        }
        storage.load(_, _) >> {
            return new Document(["@graph": [["@id": id, "foo": "bar"]]])
        }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus() == SC_NOT_FOUND
    }

    def "GET document with If-None-Match equal to ETag should return 304 Not Modified"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def doc = new Document(["@graph": [["@id": id]]])
        doc.setModified(new Date())
        def etag = doc.getChecksum()
        request.getPathInfo() >> { '/' + id }
        request.getHeader("Accept") >> { "*/*" }
        request.getHeader("If-None-Match") >> { etag }
        storage.load(_, _) >> { return doc }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED
    }

    @Unroll
    def "GET should return correct contentType"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getPathInfo() >> {
            "/${id}${view}${ending}".toString()
        }
        request.getHeader("Accept") >> {
            acceptContentType
        }
        storage.load(_, _) >> {
            new Document(["@graph": [
                    ["@id": id, "mainEntity": ["@id": "main"]],
                    ["@id": "main", "foo": "bar"]]])
        }
        storage.loadEmbellished(_, _) >> {
            new Document(["@graph": [
                    ["@id": id, "mainEntity": ["@id": "main"]],
                    ["@id": "main", "foo": "embellished"]]])
        }
        crud.doGet(request, response)

        expect:
        response.getStatus() == status
        response.getContentType() == responseContentType

        where:
        view         | ending   | acceptContentType      || responseContentType   | status
        ''           |''        | '*/*'                  || 'application/ld+json' | SC_OK
        ''           |''        | 'application/ld+json'  || 'application/ld+json' | SC_OK
        ''           |''        | 'application/json'     || 'application/json'    | SC_OK
        ''           |'.jsonld' | '*/*'                  || 'application/ld+json' | SC_OK

        '/data'      |''        | '*/*'                  || 'application/ld+json' | SC_OK
        '/data'      |''        | 'application/ld+json'  || 'application/ld+json' | SC_OK
        '/data'      |''        | 'application/json'     || 'application/json'    | SC_OK
        '/data'      |'.jsonld' | '*/*'                  || 'application/ld+json' | SC_OK
        '/data'      |'.jsonld' | 'application/json'     || 'application/ld+json' | SC_OK
        '/data'      |'.json'   | '*/*'                  || 'application/json'    | SC_OK
        '/data'      |'.json'   | 'application/json'     || 'application/json'    | SC_OK
        '/data'      |'.json'   | 'application/ld+json'  || 'application/json'    | SC_OK

        '/data-view' |''        | '*/*'                  || 'application/ld+json' | SC_OK
        '/data-view' |''        | 'application/ld+json'  || 'application/ld+json' | SC_OK
        '/data-view' |''        | 'application/json'     || 'application/json'    | SC_OK
        '/data-view' |'.jsonld' | '*/*'                  || 'application/ld+json' | SC_OK
        '/data-view' |'.jsonld' | 'application/ld+json'  || 'application/ld+json' | SC_OK
        '/data-view' |'.jsonld' | 'application/json'     || 'application/ld+json' | SC_OK
        '/data-view' |'.json'   | '*/*'                  || 'application/json'    | SC_OK
        '/data-view' |'.json'   | 'application/json'     || 'application/json'    | SC_OK
        '/data-view' |'.json'   | 'application/ld+json'  || 'application/json'    | SC_OK

        ''           |''        | ''                     || 'application/ld+json' | SC_OK
        ''           |''        | 'text/turtle'          || 'application/ld+json' | SC_OK
        ''           |''        | 'application/rdf+xml'  || 'application/ld+json' | SC_OK
        ''           |''        | 'x/x'                  || 'application/ld+json' | SC_OK
        '/data-view' |'.invalid'| '*/*'                  || null                  | SC_NOT_FOUND
        '/data'      |'.invalid'| '*/*'                  || null                  | SC_NOT_FOUND
        '/da'        |''        | '*/*'                  || 'application/ld+json' | SC_OK
    }

    @Unroll
    def "GET should format response"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getPathInfo() >> {
            "/${id}${view}${ending}".toString()
        }
        request.getHeader("Accept") >> {
            acceptContentType
        }
        storage.load(_, _) >> {
            new Document(["@graph": [
                    ["@id": id, "mainEntity": ["@id": "main"]],
                    ["@id": "main", "foo": "bar"]]])
        }
        storage.loadEmbellished(_, _) >> {
            new Document(["@graph": [
                    ["@id": id, "mainEntity": ["@id": "main"]],
                    ["@id": "main", "foo": "embellished"]]])
        }
        crud.doGet(request, response)
        String document = response.getResponseBody()

        expect:
        response.getStatus() == SC_OK
        response.getContentType() == responseContentType
        isEmbellished(document) == embellished
        isFramed(document) == framed

        where:
        view         | ending    | acceptContentType      || responseContentType   | embellished | framed
        ''           | ''        | '*/*'                  || 'application/ld+json' | true        | false
        ''           | ''        | 'application/ld+json'  || 'application/ld+json' | true        | false
        ''           | ''        | 'application/json'     || 'application/json'    | true        | true

        '/data'      | ''        | '*/*'                  || 'application/ld+json' | false       | false
        '/data'      | ''        | 'application/ld+json'  || 'application/ld+json' | false       | false
        '/data'      | '.jsonld' | '*/*'                  || 'application/ld+json' | false       | false
        '/data'      | '.jsonld' | 'application/ld+json'  || 'application/ld+json' | false       | false
        '/data'      | '.jsonld' | 'application/json'     || 'application/ld+json' | false       | false

        '/data'      | ''        | 'application/json'     || 'application/json'    | false       | true
        '/data'      | '.json'   | '*/*'                  || 'application/json'    | false       | true
        '/data'      | '.json'   | 'application/ld+json'  || 'application/json'    | false       | true

        '/data-view' | ''        | '*/*'                  || 'application/ld+json' | true        | false
        '/data-view' | ''        | 'application/ld+json'  || 'application/ld+json' | true        | false
        '/data-view' | '.jsonld' | '*/*'                  || 'application/ld+json' | true        | false
        '/data-view' | '.jsonld' | 'application/ld+json'  || 'application/ld+json' | true        | false
        '/data-view' | '.jsonld' | 'application/json'     || 'application/ld+json' | true        | false
        '/data-view' | '.jsonld' | 'x/x'                  || 'application/ld+json' | true        | false

        '/data-view' | ''        | 'application/json'     || 'application/json'    | true        | true
        '/data-view' | '.json'   | '*/*'                  || 'application/json'    | true        | true
        '/data-view' | '.json'   | 'application/ld+json'  || 'application/json'    | true        | true
        '/data-view' | '.json'   | 'x/x'                  || 'application/json'    | true        | true
    }

    @Unroll
    def "GET should format response according to parameters"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getPathInfo() >> {
            "/${id}${view}".toString()
        }
        request.getHeader("Accept") >> {
            acceptCT
        }
        request.getParameter(_) >> {
            return getParameter(queryString, arguments[0])
        }
        storage.load(_, _) >> {
            new Document(["@graph": [
                    ["@id": id, "mainEntity": ["@id": "main"]],
                    ["@id": "main", "foo": "bar"]]])
        }
        storage.loadEmbellished(_, _) >> {
            new Document(["@graph": [
                    ["@id": id, "mainEntity": ["@id": "main"]],
                    ["@id": "main", "foo": "embellished"]]])
        }

        crud.doGet(request, response)
        String document = response.getResponseBody()

        expect:

        response.getStatus() == SC_OK
        response.getContentType() == responseCT
        isEmbellished(document) == embellished
        isFramed(document) == framed

        where:
        view         | queryString                      | acceptCT || responseCT | embellished | framed
        ''           | ''                               | JSONLD   || JSONLD     | true        | false
        ''           | '?version=1'                     | JSONLD   || JSONLD     | false       | false
        ''           | '?version=1&embellished=true'    | JSONLD   || JSONLD     | false       | false
        ''           | '?framed=true'                   | JSONLD   || JSONLD     | true        | true
        ''           | '?embellished=false'             | JSONLD   || JSONLD     | false       | false
        ''           | '?embellished=false&framed=true' | JSONLD   || JSONLD     | false       | true

        ''           | ''                               | JSON     || JSON       | true        | true
        ''           | '?framed=false'                  | JSON     || JSON       | true        | false
        ''           | '?embellished=false'             | JSON     || JSON       | false       | true

        'data'       | '?embellished=true&framed=true'  | JSONLD   || JSONLD     | true        | true
        'data-view'  | '?embellished=false&framed=true' | JSONLD   || JSONLD     | false       | true

        // lens implies framed
        ''           | '?lens=card'                     | JSONLD   || JSONLD     | true        | true
        ''           | '?lens=card&framed=false'        | JSON     || JSON       | true        | true // TODO: explicitly disallow? (return 400)
    }

    @Unroll
    def "GET should use lens parameter"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getPathInfo() >> {
            "/${id}${view}".toString()
        }
        request.getHeader("Accept") >> {
            JSONLD
        }
        request.getParameter(_) >> {
            return getParameter(queryString, arguments[0])
        }
        Document d = new Document(["@graph": [["@id": "record_id",
                                               "@type": "Record",
                                               "propA": "valA",
                                               "mainEntity": ["@id": "instance_id"],
                                              ],
                                              ["@id": "instance_id",
                                               "@type": "Instance",
                                               "prop1": "val1",
                                               "prop2": "val2",
                                               "prop3": "val3",
                                               "prop4": "val4",
                                              ]]])
        storage.load(_, _) >> { d }
        storage.loadEmbellished(_, _) >> { d }
        crud.doGet(request, response)
        String document = response.getResponseBody()
        println(lens)
        println(document)
        expect:
        response.getStatus() == SC_OK
        response.getContentType() == JSONLD
        lensUsed(document) == lens

        where:
        view         | queryString                      | lens
        ''           | ''                               | 'none'
        ''           | '?lens=none'                     | 'none'
        ''           | '?lens=card'                     | 'card'
        ''           | '?lens=chip'                     | 'chip'
    }

    def "GET /<id>?lens=invalid should return 400 Bad Request"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getPathInfo() >> {
            "/${id}".toString()
        }
        request.getHeader("Accept") >> {
            "*/*"
        }
        request.getParameter("lens") >> {
            "invalid"
        }
        storage.load(_, _) >> {
            return new Document(["@graph": [["@id": id, "foo": "bar"]]])
        }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus() == SC_BAD_REQUEST
    }

    def getParameter(String queryString, String name) {
        if (queryString.startsWith('?')) {
            queryString = queryString.substring(1)
        }

        for (String s in queryString.split('&')) {
            if (s.startsWith(name + '=')) {
                return s.substring(name.length() + 1)
            }
        }
        return null
    }

    def isEmbellished(String document) {
        return document.contains('"foo":"embellished"')
    }

    def isFramed(String document) {
        if (document.startsWith('{"@id":')) {
            return true
        } else if (document.startsWith('{"@graph":')) {
            return false
        } else {
            throw new RuntimeException("Unrecognized format:" + document)
        }
    }

    def lensUsed(String document) {
        if (!document.contains('meta'))
            return 'none'
        if (document.contains('"prop3":"val3"'))
            return 'card'
        if (document.contains('"prop2":"val2"'))
            return 'chip'
        if (document.contains('"prop1":"val1"'))
            return 'token'

        throw new RuntimeException()
    }

    //TODO: Current URL structure cannot handle ids ending with /data
    @Ignore
    def "GET with id ending in /data"() {
        given:
        def id = BASE_URI.resolve("/1234/data").toString()
        request.getPathInfo() >> {
            "/${id}".toString()
        }
        storage.load(id, _) >> {
            new Document(["@graph": [["@id": id, "foo": "bar"]]])
        }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus() == SC_OK
    }

    // Tests for create
    def "POST to / should create document with generated @id"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        is.getBytes() >> {
            mapper.writeValueAsBytes(["@graph": [["@type"   : "Record",
                                                  "@id"     : "some_temporary_id",
                                                  "contains": "some new data"]]])
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            if (it.first() == "user") {
                return ["user": "SYSTEM"]
            }
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.createDocument(_, _) >> {
            Document doc = it.first()
            doc.setModified(new Date())
            return doc
        }
        when:
        crud.doPost(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_CREATED
        assert response.getHeader("Location").matches("^" + Document.getBASE_URI().toString() + "[0-9a-z]{16}\$")
    }

    def "POST to / should return 400 Bad Request on empty content"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        is.getBytes() >> {
            new byte[0]
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        when:
        crud.doPost(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_BAD_REQUEST
    }

    def "POST to / should return 400 Bad Request on form content-type"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        is.getBytes() >> {
            new String("foobar").getBytes("UTF-8")
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        request.getContentType() >> {
            "application/x-www-form-urlencoded"
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }

        when:
        crud.doPost(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_BAD_REQUEST
    }

    def "POST to / should return 403 Forbidden if user has insufficient privilege"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def postData = ["@graph": [["@id": "/some_id",
                                    "@type": "Record",
                                    "contains": "some data",
                                    "creationDate": "2002-01-08T00:00:00.0+01:00"],
                                   ["@id": "/work_id",
                                    "@type": "Work",
                                    "contains": "some new data",
                                    "heldBy":
                                            ["code": "Ting"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(postData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": false,
                                     "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        when:
        crud.doPost(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "POST to / should create document of type Item"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def postData = ["@graph": [["@id": "/some_id",
                                    "@type": "Record",
                                    "contains": "some data",
                                    "creationDate": "2002-01-08T00:00:00.0+01:00"],
                                   ["@id": "/item_id",
                                    "@type": "Item",
                                    "contains": "some new data",
                                    "heldBy":
                                            ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(postData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }

        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": false,
                                     "registrant": true]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.createDocument(_, _) >> {
            return null
        }
        when:
        crud.doPost(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_CREATED
    }

    def "POST to / should create document of type Work"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def postData = ["@graph": [["@id": "/some_id",
                                    "@type": "Record",
                                    "contains": "some data",
                                    "creationDate": "2002-01-08T00:00:00.0+01:00"],
                                   ["@id": "/work_id",
                                    "@type": "Work",
                                    "contains": "some new data",
                                    "heldBy":
                                            ["code": "S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(postData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        storage.createDocument(_, _) >> {
            return null
        }
        when:
        crud.doPost(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_CREATED
    }

    def "POST to / should create the document if at least one privilege is valid"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def postData = ["@graph": [["@id": "/some_id",
                                    "@type": "Record",
                                    "contains": "some data",
                                    "creationDate": "2002-01-08T00:00:00.0+01:00"],
                                   ["@id": "/item_id",
                                    "@type": "Item",
                                    "contains": "some new data",
                                    "heldBy":
                                            ["code": "Ting"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(postData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": true,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        storage.createDocument(_, _) >> {
            return null
        }
        storage.followDependers(_) >> {
            []
        }
        when:
        crud.doPost(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_CREATED
    }

    def "POST to / should return 400 Bad Request if no sigel found in document"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def postData = ["@graph": [["@id": "/some_id",
                                    "@type": "Record",
                                    "contains": "some data",
                                    "creationDate": "2002-01-08T00:00:00.0+01:00"],
                                   ["@id": "/item_id",
                                    "@type": "Item",
                                    "contains": "some new data"]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(postData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": true,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": true]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        storage.createDocument(_, _) >> {
            return null
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doPost(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_BAD_REQUEST
    }

    def "POST to / should return 403 Forbidden if no user information"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def postData = ["@graph": [["@id": "/some_id",
                                    "@type": "Record",
                                    "contains": "some data",
                                    "creationDate": "2002-01-08T00:00:00.0+01:00"],
                                   ["@id": "/item_id",
                                    "@type": "Item",
                                    "contains": "some new data",
                                    "heldBy":
                                            ["code": "S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(postData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return null
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        storage.createDocument(_, _) >> {
            return null
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doPost(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "POST to / should return 403 Forbidden if document not is a holding"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def postData = ["@graph": [["@id": "/some_id",
                                    "@type": "Record",
                                    "contains": "some data",
                                    "creationDate": "2002-01-08T00:00:00.0+01:00"],
                                   ["@id": "/instance_id",
                                    "@type": "Instance",
                                    "contains": "some new data",
                                    "heldBy":
                                            ["code": "S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(postData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": false,
                                     "registrant": true]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        storage.createDocument(_, _) >> {
            return null
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        when:
        crud.doPost(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "POST to / should return 400 Bad Request if unable to check access"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def postData = ["@graph": [["@id": "/some_id",
                                    "@type": "Record",
                                    "contains": "some data",
                                    "creationDate": "2002-01-08T00:00:00.0+01:00"],
                                   ["@id": "/item_id",
                                    "@type": "Item",
                                    "contains": "some new data",
                                    "heldBy":
                                            ["code": "Ting"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(postData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": false,
                                     "registrant": true]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        accessControl.checkDocumentToPost(_, _) >> {
            throw new ModelValidationException("Could not validate model.")
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doPost(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_BAD_REQUEST
    }

    def "POST to / should create non-holding if user has kat permission"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def postData = ["@graph": [["@id": "/some_id",
                                    "@type": "Record",
                                    "contains": "some data",
                                    "creationDate": "2002-01-08T00:00:00.0+01:00"],
                                   ["@id": "/work_id",
                                    "@type": "Work",
                                    "contains": "some new data",
                                    "heldBy":
                                            ["code": "S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(postData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": true,
                                     "registrant": false],
                                    ["code": "S",
                                     "cataloger": false,
                                     "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        storage.createDocument(_, _) >> {
            return null
        }
        when:
        crud.doPost(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_CREATED
    }

    def "POST to / should return 403 Forbidden if missing kat permission"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def postData = ["@graph": [["@id": "/some_id",
                                    "@type": "Record",
                                    "contains": "some data",
                                    "creationDate": "2002-01-08T00:00:00.0+01:00"],
                                   ["@id": "/work_id",
                                    "@type": "Work",
                                    "contains": "some new data",
                                    "heldBy":
                                            ["code": "S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(postData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": false,
                                     "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        storage.createDocument(_, _) >> {
            return null
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        when:
        crud.doPost(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "POST to / should create holding if user has kat permission for code"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def postData = ["@graph": [["@id": "/some_id",
                                    "@type": "Record",
                                    "contains": "some data",
                                    "creationDate": "2002-01-08T00:00:00.0+01:00"],
                                   ["@id": "/work_id",
                                    "@type": "Item",
                                    "contains": "some new data",
                                    "heldBy":
                                            ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(postData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": false],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        storage.createDocument(_, _) >> {
            return null
        }
        when:
        crud.doPost(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_CREATED
    }

    def "POST to / should create holding if user has global registrant permission for active sigel"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def postData = ["@graph": [["@id": "/some_id",
                                    "@type": "Record",
                                    "contains": "some data",
                                    "creationDate": "2002-01-08T00:00:00.0+01:00"],
                                   ["@id": "/work_id",
                                    "@type": "Item",
                                    "contains": "some new data",
                                    "heldBy":
                                            ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(postData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "SEK",
                    "permissions": [["code": "T",
                                     "cataloger": false,
                                     "registrant": false],
                                    ["code": "SEK",
                                     "cataloger": false,
                                     "registrant": false,
                                     "global_registrant": true],
                                    ]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        storage.createDocument(_, _) >> {
            return null
        }
        when:
        crud.doPost(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_CREATED
    }

    def "POST to / should return 403 Forbidden create holding if user is global registrant but not active"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def postData = ["@graph": [["@id": "/some_id",
                                    "@type": "Record",
                                    "contains": "some data",
                                    "creationDate": "2002-01-08T00:00:00.0+01:00"],
                                   ["@id": "/work_id",
                                    "@type": "Item",
                                    "contains": "some new data",
                                    "heldBy":
                                            ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(postData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "T",
                    "permissions": [["code": "T",
                                     "cataloger": false,
                                     "registrant": false],
                                    ["code": "SEK",
                                     "cataloger": false,
                                     "registrant": false,
                                     "global_registrant": true],
                    ]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        storage.createDocument(_, _) >> {
            return null
        }
        when:
        crud.doPost(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "POST to / should return 403 Forbidden if missing kat permission for code"() {
        def is = GroovyMock(ServletInputStream.class)
        def postData = ["@graph": [["@id": "/some_id",
                                    "@type": "Record",
                                    "contains": "some data",
                                    "creationDate": "2002-01-08T00:00:00.0+01:00"],
                                   ["@id": "/work_id",
                                    "@type": "Item",
                                    "contains": "some new data",
                                    "heldBy":
                                            ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(postData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": true,
                                     "registrant": false],
                                    ["code": "S",
                                     "cataloger": false,
                                     "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        storage.createDocument(_, _) >> {
            return null
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doPost(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    // Tests for update

    def "PUT to /<id> should return 400 Bad Request on empty content"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        is.getBytes() >> {
            new byte[0]
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/some_id"
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        when:
        crud.doPut(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_BAD_REQUEST
    }

    def "PUT to /<id> should return 400 Bad Request on form content-type"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        is.getBytes() >> {
            new String("foobar").getBytes("UTF-8")
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/some_id"
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/x-www-form-urlencoded"
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }

        when:
        crud.doPut(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_BAD_REQUEST
    }

    def "PUT to /<id> should update document if it exists"() {
        given:
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = BASE_URI.resolve("/1234").toString()
        def oldContent = ["@graph": [["@id": id,
                                      "@type": "Record",
                                      "contains": "some data"]]]
        def newContent = ["@graph": [["@id": id,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some updated data"]]]
        def is = GroovyMock(ServletInputStream.class)
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/1234"
        }
        request.getMethod() >> {
            "PUT"
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            if (it.first() == "user") {
                return ["user": "SYSTEM"]
            }
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            Document doc = new Document(newContent)
            doc.setModified(modifiedDate)
            doc.setCreated(createdDate)
            return doc
        }
        when:
        crud.doPut(request, response)
        then:
        // FIXME assert created/modified
        assert response.getStatus() == HttpServletResponse.SC_NO_CONTENT
    }

    def "PUT to /<alternate_id> should return 302 Found"() {
        given:
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = BASE_URI.resolve("/1234").toString()
        def altId = BASE_URI.resolve("/alt1234").toString()
        def oldContent = ["@graph": [["@id": id,
                                      "@type": "Record",
                                      "sameAs": [["@id": altId]],
                                      "contains": "some data"]]]
        def newContent = ["@graph": [["@id": id,
                                      "@type": "Record",
                                      "sameAs": [["@id": altId]],
                                      "created": createdDate,
                                      "contains": "some updated data"]]]
        def is = GroovyMock(ServletInputStream.class)
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            altId
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            if (it.first() == "user") {
                return ["user": "SYSTEM"]
            }
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(altId) >> {
            return null
        }
        storage.loadDocumentByMainId(id) >> {
            return new Document(oldContent)
        }
        storage.getMainId(_) >> {
            return id
        }
        storage.getIdType(_) >> {
            return IdType.RecordSameAsId
        }
        storage.createDocument(_, _) >> {
            Document doc = new Document(newContent)
            doc.setModified(modifiedDate)
            doc.setCreated(createdDate)
            return doc
        }
        when:
        crud.doPut(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_FOUND
    }

    def "PUT to /<id> should return 404 Not Found if document does not exist"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def id = BASE_URI.resolve("/bad_id").toString()
        def putData = ["@graph": [["@id": id,
                                   "@type": "Record",
                                   "contains": "some new data"]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(putData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/bad_id"
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            if (it.first() == "user") {
                return ["user": "SYSTEM"]
            }
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        when:
        crud.doPut(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_NOT_FOUND
    }

    def "PUT to /<id> should return 400 Bad Request if ID in document is missing"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def id = BASE_URI.resolve("/some_id").toString()
        def putData = ["@graph": [["@type": "Record",
                                   "contains": "some new data"]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(putData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            if (it.first() == "user") {
                return ["user": "SYSTEM"]
            }
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return null
        }
        storage.getMainId(_) >> {
            return null
        }
        when:
        crud.doPut(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_BAD_REQUEST
    }

    def "PUT to /<id> should return 400 Bad Request if record ID has been changed"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def id = BASE_URI.resolve("/some_id").toString()
        def doc = ["@graph": [["@id": id,
                               "@type": "Record",
                               "contains": "some new data"]]]
        def putData = ["@graph": [["@id": id + "_changed",
                                   "@type": "Record",
                                   "contains": "some new data"]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(putData)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            "/some_other_id"
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            if (it.first() == "user") {
                return ["user": "SYSTEM"]
            }
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(_, _) >> {
            return new Document(doc)
        }
        storage.getMainId(_) >> {
            return id
        }
        storage.getIdType(_) >> {
            return IdType.RecordMainId
        }
        when:
        crud.doPut(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_BAD_REQUEST
    }

    def "PUT to /<id> should return 403 Forbidden if user has insufficient privilege"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "Ting", "@id":"https://libris.kb.se/library/Ting"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "Ting", "@id":"https://libris.kb.se/library/Ting"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": false],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doPut(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "PUT to /<id> should return 403 Forbidden if it is mismatch between sigel in the documents"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "Ting", "@id":"https://libris.kb.se/library/Ting"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": false,
                                     "registrant": true]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doPut(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "PUT to /<id> should return 403 Forbidden if new content is not a holding"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "Ting"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/workId",
                                      "@type": "Work",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "Ting"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": false,
                                     "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        when:
        crud.doPut(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "PUT to /<id> should return 403 Forbidden if old content is not a holding"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/instanceId",
                                      "@type": "Instance",
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "Ting"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/ItemId",
                                      "@type": "Item",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "Ting"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": false,
                                     "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        when:
        crud.doPut(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "PUT to /<id> should return 403 Forbidden if no match on sigel in user privileges"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "Foo", "@id":"https://libris.kb.se/library/Foo"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "Foo", "@id":"https://libris.kb.se/library/Foo"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": true,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": true]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doPut(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "PUT to /<id> should return 403 Forbidden if missing sigel in old document and no permission for new"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some other data"]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "Ting", "@id":"https://libris.kb.se/library/Ting"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Sing",
                    "permissions": [["code": "Sing",
                             "cataloger": false,
                             "registrant": true],
                            ["code": "S",
                             "cataloger": true,
                             "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doPut(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "PUT to /<id> - Registrant can correct missing sigel in holding"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some other data"]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "Ting", "@id":"https://libris.kb.se/library/Ting"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": true,
                                     "global_registrant": false]]
                    ]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        when:
        crud.doPut(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_NO_CONTENT
    }


    def "PUT to /<id> - Global registrant can correct missing sigel in holding"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some other data"]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "Sing", "@id":"https://libris.kb.se/library/Sing"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "SEK",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": true,
                                     "global_registrant": false],
                                    ["code": "SEK",
                                     "cataloger": false,
                                     "registrant": false,
                                     "global_registrant": true]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        when:
        crud.doPut(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_NO_CONTENT
    }

    def "PUT to /<id> should return 400 Bad Request if missing sigel in new document"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some new other data"]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doPut(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_BAD_REQUEST
    }

    def "PUT to /<id> should return 403 Forbidden if no user information"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return null
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        when:
        crud.doPut(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "PUT to /<id> should update content of type holding"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "Ting", "@id":"https://libris.kb.se/library/Ting"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "Ting", "@id":"https://libris.kb.se/library/Ting"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": false,
                                     "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        when:
        crud.doPut(request, response)
        then:
        // FIXME assert created/modified
        assert response.getStatus() == HttpServletResponse.SC_NO_CONTENT
    }

    def "PUT to /<id> should update content of type Work"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/workId",
                                      "@type": "Work",
                                      "contains": "some other data"]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/workId",
                                      "@type": "Work",
                                      "contains": "some new other data"]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getParameter("collection") >> {
            "bib"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        when:
        crud.doPut(request, response)
        then:
        // FIXME assert created/modified
        assert response.getStatus() == HttpServletResponse.SC_NO_CONTENT
    }

    def "PUT to /<id> should return 400 Bad Request if unable to check access"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        accessControl.checkDocumentToPut(_, _, _, _) >> {
            throw new ModelValidationException("Could not validate model.")
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }

        when:
        crud.doPut(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_BAD_REQUEST
    }

    def "PUT to /<id> should return 403 Forbidden if missing kat permission"() {
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/workId",
                                      "@type": "Work",
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/workId",
                                      "@type": "Work",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["authorization": [["code": "S",
                                       "cataloger": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doPut(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "PUT to /<id> should update holding if user has kat permission for code"() {
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/workId",
                                      "@type": "Work",
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/workId",
                                      "@type": "Work",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "S",
                    "permissions": [["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        when:
        crud.doPut(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_NO_CONTENT
    }

    def "PUT to /<id> should return 403 Forbidden if missing kat permission for code"() {
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/workId",
                                      "@type": "Item",
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/workId",
                                      "@type": "Item",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doPut(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }


    def "PUT to /<id> should update holding if user has global registrant permission active"() {
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "SEK",
                    "permissions": [["code": "T",
                                     "cataloger": false,
                                     "registrant": false],
                                    ["code": "SEK",
                                     "cataloger": false,
                                     "registrant": false,
                                     "global_registrant": true],
                    ]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        when:
        crud.doPut(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_NO_CONTENT
    }

    def "PUT to /<id> should return 403 Forbidden if user has global registrant permission but not active"() {
        def is = GroovyMock(ServletInputStream.class)
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        def id = "/1234"
        def fullId = BASE_URI.resolve(id).toString()
        def oldContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "contains": "some data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "T",
                    "permissions": [["code": "T",
                                     "cataloger": false,
                                     "registrant": false],
                                    ["code": "SEK",
                                     "cataloger": false,
                                     "registrant": false,
                                     "global_registrant": true],
                    ]]
        }
        request.getRequestURL() >> {
            return new StringBuffer(BASE_URI.toString())
        }
        storage.load(_, _) >> {
            Document doc = new Document(oldContent)
            doc.setCreated(Date.parse(dateFormat, createdDate))
            return doc
        }
        storage.createDocument(_, _) >> {
            throw new Exception("This shouldn't happen")
        }
        when:
        crud.doPut(request, response)
        then:
        assert response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    // Tests for delete

    def "DELETE to /<id> should delete document if it exists when user set to SYSTEM"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def data = ["@graph": [["@id": id,
                                "@type": "Record",
                                "contains": "some new data"]]]
        request.getPathInfo() >> {
            "/1234"
        }
        request.getMethod() >> {
            "DELETE"
        }
        request.getAttribute(_) >> {
            if (it.first() == "user") {
                return ["user": "SYSTEM"]
            }
        }
        storage.load(_, _) >> {
            new Document(data)
        }
        storage.remove(_, _) >> {
          return true
        }
        storage.followDependers(_) >> {
            []
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_NO_CONTENT
    }

    def "DELETE to /<id> should return 404 Not Found if document does not exist"() {
        given:
        request.getPathInfo() >> {
            "/dataset/some_document"
        }
        request.getMethod() >> {
            "DELETE"
        }
        request.getAttribute(_) >> {
            if (it.first() == "user") {
                return ["user": "SYSTEM"]
            }
        }
        storage.load(_, _) >> {
          return null
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_NOT_FOUND
    }

    def "DELETE to /<id> should return 410 Gone on already deleted document"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def data = ["@graph": [["@id": id,
                                "@type": "Record",
                                "contains": "some new data"]]]
        request.getPathInfo() >> {
            "/1234"
        }
        request.getMethod() >> {
            "DELETE"
        }
        request.getAttribute(_) >> {
            if (it.first() == "user") {
                return ["user": "SYSTEM"]
            }
        }
        storage.load(_, _) >> {
            def doc = new Document(data)
            doc.deleted = true
            return doc
        }
        storage.remove(_, _) >> {
          return true
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_GONE
    }

    def "DELETE to /<id> should return 403 Forbidden if user has insufficient privilege"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def itemId = BASE_URI.resolve("/1234#it").toString()
        def data = ["@graph": [["@id": id,
                                "@type": "Record",
                                "creationDate": "2002-01-08T00:00:00.0+01:00"],
                                ["@id": itemId,
                                 "@type": "Item",
                                 "contains": "some new data",
                                 "heldBy":
                                         ["code": "Ting", "@id":"https://libris.kb.se/library/Ting"]]]
        ]
        request.getPathInfo() >> {
            "/1234"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": false],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        request.getMethod() >> {
            "DELETE"
        }
        storage.load(_, _) >> {
            new Document(data)
        }
        storage.remove(_, _) >> {
            return true
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "DELETE to /<id> should return 403 Forbidden if document not is a holding "() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def workId = BASE_URI.resolve("/1234#it").toString()
        def data = ["@graph": [["@id": id,
                                "@type": "Record",
                                "creationDate": "2002-01-08T00:00:00.0+01:00"],
                               ["@id": workId,
                                "@type": "Work",
                                "contains": "some new data",
                                "heldBy":
                                        ["code": "Ting"]]]]

        request.getPathInfo() >> {
            "/1234"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": false,
                                     "registrant": false]]]
        }
        request.getMethod() >> {
            "DELETE"
        }
        storage.load(_, _) >> {
            new Document(data)
        }
        storage.remove(_, _) >> {
            return true
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "DELETE to /<id> should return 403 Forbidden if no user information"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def itemId = BASE_URI.resolve("/1234#it").toString()
        def data = ["@graph": [["@id": id,
                                "@type": "Record",
                                "creationDate": "2002-01-08T00:00:00.0+01:00"],
                               ["@id": itemId,
                                "@type": "Item",
                                "contains": "some new data",
                                "heldBy":
                                        ["code": "Ting"]]]]
        request.getPathInfo() >> {
            "/1234"
        }
        request.getAttribute(_) >> {
            return null
        }
        request.getMethod() >> {
            "DELETE"
        }
        storage.load(_, _) >> {
            new Document(data)
        }
        storage.remove(_, _) >> {
            return true
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "DELETE to /<id> should return 400 Bad Request if no sigel found in document"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def itemId = BASE_URI.resolve("/1234#it").toString()
        def data = ["@graph": [["@id": id,
                                "@type": "Record",
                                "creationDate": "2002-01-08T00:00:00.0+01:00"],
                               ["@id": itemId,
                                "@type": "Item",
                                "contains": "some new data"]]]
        request.getPathInfo() >> {
            "/1234"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": true,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        request.getMethod() >> {
            "DELETE"
        }
        storage.load(_, _) >> {
            new Document(data)
        }
        storage.remove(_, _) >> {
            return true
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_BAD_REQUEST
    }

    def "DELETE to /<id> should delete the document of type Item"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def itemId = BASE_URI.resolve("/1234#it").toString()
        def data = ["@graph": [["@id": id,
                                "@type": "Record",
                                "creationDate": "2002-01-08T00:00:00.0+01:00"],
                               ["@id": itemId,
                                "@type": "Item",
                                "contains": "some new data",
                                "heldBy":
                                        ["code": "Ting", "@id":"https://libris.kb.se/library/S"]]]]
        request.getPathInfo() >> {
            "/1234"
        }
        request.getMethod() >> {
            "DELETE"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": true,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        storage.load(_, _) >> {
            new Document(data)
        }
        storage.remove(_, _) >> {
            return true
        }
        storage.followDependers(_) >> {
            []
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_NO_CONTENT
    }

    def "DELETE to /<id> should delete the document if at least one privilege is valid"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def instanceId = BASE_URI.resolve("/1234#it").toString()
        def data = ["@graph": [["@id": id,
                                "@type": "Record",
                                "creationDate": "2002-01-08T00:00:00.0+01:00"],
                               ["@id": instanceId,
                                "@type": "Instance",
                                "contains": "some new data",
                                "heldBy":
                                        ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        request.getPathInfo() >> {
            "/1234"
        }
        request.getMethod() >> {
            "DELETE"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": true,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        storage.load(_, _) >> {
            new Document(data)
        }
        storage.remove(_, _) >> {
            return true
        }
        storage.followDependers(_) >> {
            []
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_NO_CONTENT
    }

    def "DELETE to /<id> should delete the document of type Work"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def workId = BASE_URI.resolve("/1234#it").toString()
        def data = ["@graph": [["@id": id,
                                "@type": "Record",
                                "creationDate": "2002-01-08T00:00:00.0+01:00"],
                               ["@id": workId,
                                "@type": "Work",
                                "contains": "some new data",
                                "heldBy":
                                        ["code": "S"]]]]
        request.getPathInfo() >> {
            "/1234"
        }
        request.getMethod() >> {
            "DELETE"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": true,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        storage.load(_, _) >> {
            new Document(data)
        }
        storage.remove(_, _) >> {
            return true
        }
        storage.followDependers(_) >> {
            []
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_NO_CONTENT
    }

    def "DELETE to /<id> should redirect if legacy id"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def workId = BASE_URI.resolve("/1234#it").toString()
        def redirectId = BASE_URI.resolve("/5678").toString()
        def data = ["@graph": [["@id": id,
                                "@type": "Record",
                                "creationDate": "2002-01-08T00:00:00.0+01:00",
                                "sameAs": [["@id": redirectId]]],
                               ["@id": workId,
                                "@type": "Work",
                                "contains": "some new data",
                                "heldBy":
                                        ["code": "S"]]]]
        request.getPathInfo() >> {
            redirectId
        }
        request.getMethod() >> {
            "DELETE"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
                                     "cataloger": true,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        storage.load(_, _) >> {
            return null
        }
        storage.loadDocumentByMainId(redirectId) >> {
            return null
        }
        storage.loadDocumentByMainId(id) >> {
            return new Document(data)
        }
        storage.getMainId(_) >> {
            return id
        }
        storage.getIdType(_) >> {
            return IdType.RecordSameAsId
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_FOUND
    }

    def "DELETE to /<id> should return 400 Bad Request if unable to check permissions"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def workId = BASE_URI.resolve("/1234#it").toString()
        def data = ["@graph": [["@id": id,
                                "@type": "Record",
                                "creationDate": "2002-01-08T00:00:00.0+01:00"],
                               ["@id": workId,
                                "@type": "Work",
                                "contains": "some new data",
                                "heldBy":
                                        ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        request.getPathInfo() >> {
            "/1234"
        }
        request.getMethod() >> {
            "DELETE"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
                                     "cataloger": true,
                                     "registrant": true],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        storage.load(_, _) >> {
            new Document(data)
        }
        accessControl.checkDocumentToDelete(_, _, _) >> {
            throw new ModelValidationException("Could not validate model.")
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_BAD_REQUEST
    }

    def "DELETE to /<id> should delete non-holding if user has kat permission"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def itemId = BASE_URI.resolve("/1234#it").toString()
        def data = ["@graph": [["@id": id,
                                "@type": "Record",
                                "creationDate": "2002-01-08T00:00:00.0+01:00"],
                               ["@id": itemId,
                                "@type": "Instance",
                                "contains": "some new data",
                                "heldBy":
                                        ["code": "Ting"]]]]
        request.getPathInfo() >> {
            "/1234"
        }
        request.getMethod() >> {
            "DELETE"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": false],
                                    ["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        storage.load(_, _) >> {
            new Document(data)
        }
        storage.remove(_, _) >> {
            return true
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        storage.followDependers(_) >> {
            []
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_NO_CONTENT
    }

    def "DELETE to /<id> should return 403 Forbidden if missing kat permission"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def itemId = BASE_URI.resolve("/1234#it").toString()
        def data = ["@graph": [["@id": id,
                                "@type": "Record",
                                "creationDate": "2002-01-08T00:00:00.0+01:00"],
                               ["@id": itemId,
                                "@type": "Instance",
                                "contains": "some new data",
                                "heldBy":
                                        ["code": "Ting", "@id":"https://libris.kb.se/library/Ting"]]]]
        request.getPathInfo() >> {
            "/1234"
        }
        request.getMethod() >> {
            "DELETE"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": false],
                                    ["code": "S",
                                     "cataloger": false,
                                     "registrant": false]]]
        }
        storage.load(_, _) >> {
            new Document(data)
        }
        storage.remove(_, _) >> {
            return true
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

    def "DELETE to /<id> should delete holding if user has kat permission for code"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def itemId = BASE_URI.resolve("/1234#it").toString()
        def data = ["@graph": [["@id": id,
                                "@type": "Record",
                                "creationDate": "2002-01-08T00:00:00.0+01:00"],
                               ["@id": itemId,
                                "@type": "Item",
                                "contains": "some new data",
                                "heldBy":
                                        ["code": "S", "@id":"https://libris.kb.se/library/S"]]]]
        request.getPathInfo() >> {
            "/1234"
        }
        request.getMethod() >> {
            "DELETE"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "S",
                    "permissions": [["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        storage.load(_, _) >> {
            new Document(data)
        }
        storage.remove(_, _) >> {
            return true
        }
        storage.followDependers(_) >> {
            []
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_NO_CONTENT
    }

    def "DELETE to /<id> should return 403 Forbidden if missing kat permission for code"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def itemId = BASE_URI.resolve("/1234#it").toString()
        def data = ["@graph": [["@id": id,
                                "@type": "Record",
                                "creationDate": "2002-01-08T00:00:00.0+01:00"],
                               ["@id": itemId,
                                "@type": "Item",
                                "contains": "some new data",
                                "heldBy":
                                        ["code": "Ting", "@id":"https://libris.kb.se/library/S"]]]]
        request.getPathInfo() >> {
            "/1234"
        }
        request.getMethod() >> {
            "DELETE"
        }
        request.getAttribute(_) >> {
            return ["active_sigel": "Ting",
                    "permissions": [["code": "Ting",
                                     "cataloger": false,
                                     "registrant": false],
                                    ["code": "S",
                                     "cataloger": false,
                                     "registrant": false]]]
        }
        storage.load(_, _) >> {
            new Document(data)
        }
        storage.remove(_, _) >> {
            return true
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        when:
        crud.doDelete(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_FORBIDDEN
    }

}
