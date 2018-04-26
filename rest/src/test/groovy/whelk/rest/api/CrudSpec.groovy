package whelk.rest.api

import org.codehaus.jackson.map.ObjectMapper
import spock.lang.Ignore
import spock.lang.Specification
import whelk.Document
import whelk.IdType
import whelk.JsonLd
import whelk.Location
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.exception.ModelValidationException
import whelk.exception.StorageCreateFailedException
import whelk.exception.WhelkRuntimeException
import whelk.rest.security.AccessControl
import whelk.util.LegacyIntegrationTools

import javax.servlet.ServletInputStream
import javax.servlet.ServletOutputStream
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import javax.servlet.http.HttpServletResponseWrapper

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
    LegacyIntegrationTools legacyIntegrationTools
    static final URI BASE_URI = Document.BASE_URI
    private static final ObjectMapper mapper = new ObjectMapper()


    void setup() {
        request = GroovyMock(HttpServletRequest.class)
        ServletOutputStream out = GroovyMock(ServletOutputStream.class)
        response = new HttpServletResponseWrapper(GroovyMock(HttpServletResponse.class)) {
            int status = 0
            String contentType
            def headers = [:]

            public ServletOutputStream getOutputStream() {
                return out
            }

            public void sendError(int sc, String mess) {
                this.status = sc
            }

            public void setHeader(String h, String v) {
                headers.put(h, v)
            }

            public String getHeader(String h) {
                return headers.get(h)
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
        whelk.displayData = ['lensGroups': ['chips': [:]]]
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
            id
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

    def "GET /<sameAs ID> should return 302 Found"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def altId = BASE_URI.resolve("/alt_1234").toString()
        request.pathInfo >> {
            altId
        }
        request.getRequestURI() >> {
            altId
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
        request.getRequestURI() >> {
            id
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
        request.getRequestURI() >> {
            altId
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
        request.getRequestURI() >> {
            id
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
        request.getRequestURI() >> {
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

    def "GET /<id>?version=1 should return 404 Not Found if document can't be found"() {
        given:
        def id = BASE_URI.resolve("/1234?version=1").toString()
        request.getRequestURI() >> {
            id
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

    def "GET /<id>/data should return raw representation"() {
        // We don't allow this for now
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getRequestURI() >> {
            "#{id}/data".toString()
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
        request.getRequestURI() >> {
            "#{id}/data.jsonld".toString()
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
        request.getRequestURI() >> {
            "#{id}/data.json".toString()
        }
        request.getHeader("Accept") >> {
            "*/*"
        }
        storage.load(_, _) >> {
            new Document(["@graph": [["@id": id,
                                      "foo": "bar",
                                      "baz": [
                                        "@id": "examplevocab:"
                                      ],
                                      "quux": [
                                        "@id": "some_term"
                                      ],
                                      "bad_ref": [
                                        "@id": "invalid:ref"
                                      ]]]])
        }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_OK
        response.getContentType() == "application/json"
    }

    def "GET /<id>/data.ttl should return 406 Not Acceptable"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getRequestURI() >> {
            "#{id}/data.ttl".toString()
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
        response.getStatus() == HttpServletResponse.SC_NOT_ACCEPTABLE
    }

    def "GET /<id>/data.rdf should return 406 Not Acceptable"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        request.getRequestURI() >> {
            "#{id}/data.rdf".toString()
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
        response.getStatus() == HttpServletResponse.SC_NOT_ACCEPTABLE
    }

    def "GET document with If-None-Match equal to ETag should return 304 Not Modified"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def doc = new Document(["@graph": [["@id": id]]])
        doc.setModified(new Date())
        def etag = doc.getModified()
        request.getRequestURI() >> { id }
        request.getHeader("Accept") >> { "*/*" }
        request.getHeader("If-None-Match") >> { etag }
        storage.load(_, _) >> { return doc }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
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
        request.getRequestURI() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        accessControl.isValidActiveSigel(_) >> {
            return true
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
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
        request.getRequestURI() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        accessControl.isValidActiveSigel(_) >> {
            return true
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
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
        request.getRequestURI() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        accessControl.isValidActiveSigel(_) >> {
            return true
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
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
        storage.getDependers(_) >> {
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
        request.getRequestURI() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
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
        accessControl.isValidActiveSigel(_) >> {
            return true
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
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
        request.getRequestURI() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
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
        accessControl.isValidActiveSigel(_) >> {
            return true
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
        request.getRequestURI() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        accessControl.isValidActiveSigel(_) >> {
            return true
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
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
        request.getRequestURI() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
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
        request.getRequestURI() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        accessControl.isValidActiveSigel(_) >> {
            return true
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
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
        request.getRequestURI() >> {
            "/"
        }
        request.getMethod() >> {
            "POST"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
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
        request.getRequestURI() >> {
            "/"
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
        request.getRequestURI() >> {
            "/"
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
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

    def "PUT to /<id> should return 403 Forbidden if missing sigel in old document"() {
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
        request.getRequestURI() >> {
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
        response.getStatus() == HttpServletResponse.SC_FORBIDDEN
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
                                              ["code": "S"]]]]
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
        request.getRequestURI() >> {
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
                                              ["code": "S"]]]]
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        accessControl.isValidActiveSigel(_) >> {
            return true
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "hold"
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
                                      "contains": "some other data",
                                      "heldBy":
                                              ["code": "S"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/workId",
                                      "@type": "Work",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getRequestURI() >> {
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
        accessControl.isValidActiveSigel(_) >> {
            return true
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
                                              ["code": "S"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getRequestURI() >> {
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
        accessControl.checkDocumentToPut(_, _, _) >> {
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
                                              ["code": "S"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/workId",
                                      "@type": "Work",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getRequestURI() >> {
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
                                       "xlreg": false]]]
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
                                              ["code": "S"]]]]
        def newContent = ["@graph": [["@id": fullId,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/workId",
                                      "@type": "Work",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["code": "S"]]]]
        is.getBytes() >> {
            mapper.writeValueAsBytes(newContent)
        }
        request.getInputStream() >> {
            is
        }
        request.getPathInfo() >> {
            id
        }
        request.getRequestURI() >> {
            id
        }
        request.getMethod() >> {
            "PUT"
        }
        accessControl.isValidActiveSigel(_) >> {
            return true
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        request.getContentType() >> {
            "application/ld+json"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "S",
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
        request.getRequestURI() >> {
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


    // Tests for delete

    def "DETETE to /<id> should delete document if it exists when user set to SYSTEM"() {
        given:
        def id = BASE_URI.resolve("/1234").toString()
        def data = ["@graph": [["@id": id,
                                "@type": "Record",
                                "contains": "some new data"]]]
        request.getPathInfo() >> {
            "/1234"
        }
        request.getRequestURI() >> {
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
        storage.getDependers(_) >> {
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
            "/1234"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
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
        request.getRequestURI() >> {
            "/1234"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
            "/1234"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
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
        accessControl.isValidActiveSigel(_) >> {
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
        request.getRequestURI() >> {
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
        storage.remove(_, _) >> {
            return true
        }
        storage.getDependers(_) >> {
            []
        }
        accessControl.isValidActiveSigel(_) >> {
            return true
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
        request.getRequestURI() >> {
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
        storage.remove(_, _) >> {
            return true
        }
        storage.getDependers(_) >> {
            []
        }
        accessControl.isValidActiveSigel(_) >> {
            return true
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
        request.getRequestURI() >> {
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
        storage.remove(_, _) >> {
            return true
        }
        storage.getDependers(_) >> {
            []
        }
        accessControl.isValidActiveSigel(_) >> {
            return true
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
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
        request.getRequestURI() >> {
            "/1234"
        }
        request.getMethod() >> {
            "DELETE"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
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
        accessControl.isValidActiveSigel(_) >> {
            return true
        }
        LegacyIntegrationTools.determineLegacyCollection(_, _) >> {
            return "bib"
        }
        storage.getDependers(_) >> {
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
        request.getRequestURI() >> {
            "/1234"
        }
        request.getMethod() >> {
            "DELETE"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
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
                                        ["code": "Ting", "@id":"https://libris.kb.se/library/S"]]]]
        request.getPathInfo() >> {
            "/1234"
        }
        request.getRequestURI() >> {
            "/1234"
        }
        request.getMethod() >> {
            "DELETE"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "S",
                                     "cataloger": true,
                                     "registrant": false]]]
        }
        storage.load(_, _) >> {
            new Document(data)
        }
        storage.remove(_, _) >> {
            return true
        }
        storage.getDependers(_) >> {
            []
        }
        accessControl.isValidActiveSigel(_) >> {
            return true
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
        request.getRequestURI() >> {
            "/1234"
        }
        request.getMethod() >> {
            "DELETE"
        }
        request.getAttribute(_) >> {
            return ["permissions": [["code": "Ting",
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


    /*
     * Utilities tests
     *
     */

    def "should get ID from path"() {
        expect:
        Crud.getIdFromPath(path) == id
        where:
        path                                | id
        ""                                  | null
        "/"                                 | null
        "/foo"                              | "foo"
        "/foo/data"                         | "foo"
        "/foo/data.jsonld"                  | "foo"
        "/foo/data.json"                    | "foo"
        "/foo/data-view.jsonld"             | "foo"
        "/foo/data-view.json"               | "foo"
        "/https://example.com/some/id"      | "https://example.com/some/id"
        "/https://example.com/some/id/data" | "https://example.com/some/id"
    }

    def "should get formatting type"() {
        expect:
        Crud.getFormattingType(path, contentType) == type
        where:
        path                                | contentType | type
        ""                                  | "application/ld+json" | Crud.FormattingType.EMBELLISHED
        "/"                                 | "application/ld+json" | Crud.FormattingType.EMBELLISHED
        "/foo"                              | "application/ld+json" | Crud.FormattingType.EMBELLISHED
        "/foo"                              | "application/json"    | Crud.FormattingType.FRAMED_AND_EMBELLISHED
        "/foo/data"                         | "application/ld+json" | Crud.FormattingType.RAW
        "/foo/data"                         | "application/json"    | Crud.FormattingType.FRAMED
        "/foo/data.jsonld"                  | "application/ld+json" | Crud.FormattingType.RAW
        "/foo/data.json"                    | "application/ld+json" | Crud.FormattingType.FRAMED
        "/foo/data-view"                    | "application/ld+json" | Crud.FormattingType.EMBELLISHED
        "/foo/data-view"                    | "application/json"    | Crud.FormattingType.FRAMED_AND_EMBELLISHED
        "/foo/data-view.jsonld"             | "application/ld+json" | Crud.FormattingType.EMBELLISHED
        "/foo/data-view.json"               | "application/ld+json" | Crud.FormattingType.FRAMED_AND_EMBELLISHED
        "/https://example.com/some/id"      | "application/ld+json" | Crud.FormattingType.EMBELLISHED
        "/https://example.com/some/id/data" | "application/ld+json" | Crud.FormattingType.RAW
        "/https://example.com/some/id/data" | "application/json"    | Crud.FormattingType.FRAMED
        "/foo/data"                         | "text/turtle"         | Crud.FormattingType.RAW
        "/foo/data"                         | "application/rdf+xml" | Crud.FormattingType.RAW
        "/foo/data-view"                    | "text/turtle"         | Crud.FormattingType.EMBELLISHED
        "/foo/data-view"                    | "application/rdf+xml" | Crud.FormattingType.EMBELLISHED
    }

    def "should throw exception when getting formatting type for invalid file ending, I"() {
        when:
        Crud.getFormattingType('/foo/data.invalid', 'application/ld+json')
        then:
        thrown WhelkRuntimeException
    }

    def "should throw exception when getting formatting type for invalid file ending, II"() {
        when:
        Crud.getFormattingType('/foo/data-view.invalid', 'application/ld+json')
        then:
        thrown WhelkRuntimeException
    }
}
