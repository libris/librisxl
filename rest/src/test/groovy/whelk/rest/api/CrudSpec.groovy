package whelk.rest.api

import org.codehaus.jackson.map.ObjectMapper
import spock.lang.Specification
import whelk.Document
import whelk.Location
import whelk.Whelk
import whelk.component.Storage

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
    Storage storage
    HttpServletRequest request
    HttpServletResponse response
    private static final ObjectMapper mapper = new ObjectMapper()


    void setup() {
        request = GroovyMock(HttpServletRequest.class)
        ServletOutputStream out = GroovyMock(ServletOutputStream.class)
        response = new HttpServletResponseWrapper(GroovyMock(HttpServletResponse.class)) {
            int status = 0
            String contentType
            public ServletOutputStream getOutputStream() {
                return out
            }
            public void sendError(int sc, String mess) {
                this.status = sc
            }
        }
        storage = GroovyMock(Storage.class)
        whelk = new Whelk(storage)
        crud = new Crud()
        crud.whelk = whelk
    }

    def "should return correct responseurl for PUT"() {
        when:
        request.getRequestURL() >> { new StringBuffer("http://localhost:8180/whelk/bib/12354") }
        String responseUrl = crud.getResponseUrl(request, "/bib/12354", "bib")
        then:
        responseUrl == "http://localhost:8180/whelk/bib/12354"
    }

    def "should return correct responseurl for POST"() {
        when:
        request.getRequestURL() >> { new StringBuffer("http://localhost:8180/whelk/bib/") }
        String responseUrl = crud.getResponseUrl(request, "/bib/12354", "bib")
        then:
        responseUrl == "http://localhost:8180/whelk/bib/12354"
    }

    def "should display requested document"() {
        given:
        request.getPathInfo() >> { "/bib/1234" }
        storage.locate(_) >> { new Location(new Document(it.first(), ["foo":"bar"], ["dataset":"bib",(Document.CONTENT_TYPE_KEY):"application/ld+json","identifier":it.first()])) }
        when:
        crud.doGet(request, response)
        then:
        response.getStatus()== 200
        response.getContentType() == "application/json"
    }

    def "should set error if empty content"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        is.getBytes() >> { new byte[0] }
        request.getInputStream() >> { is }
        request.getPathInfo() >> { "/dataset/" }
        request.getMethod() >> { "POST" }
        storage.store(_) >> { throw new Exception("This shouldn't happen") }
        when:
        crud.doPost(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_BAD_REQUEST
    }

    def "should set error if form content-type"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        is.getBytes() >> { new String("foobar").getBytes("UTF-8") }
        request.getInputStream() >> { is }
        request.getPathInfo() >> { "/dataset/" }
        request.getMethod() >> { "POST" }
        request.getContentType() >> { "application/x-www-form-urlencoded" }
        storage.store(_) >> { throw new Exception("This shouldn't happen") }

        when:
        crud.doPost(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_BAD_REQUEST
    }

    def "should set error on incorrect POST url"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        is.getBytes() >> { new String("foobar").getBytes("UTF-8") }
        request.getInputStream() >> { is }
        request.getPathInfo() >> { "/dataset/foo" }
        request.getMethod() >> { "POST" }
        request.getContentType() >> { "text/plain" }
        storage.store(_) >> { throw new Exception("This shouldn't happen") }
        when:
        crud.doPost(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_BAD_REQUEST
    }

    def "should set error on incorrect PUT url"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        is.getBytes() >> { new String("foobar").getBytes("UTF-8") }
        request.getInputStream() >> { is }
        request.getPathInfo() >> { "/" }
        request.getMethod() >> { "PUT" }
        request.getContentType() >> { "text/plain" }
        storage.store(_) >> { throw new Exception("This shouldn't happen") }
        when:
        crud.doPost(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_BAD_REQUEST

    }

    def "should deny unauthorized user"() {
        def is = GroovyMock(ServletInputStream.class)
        is.getBytes() >> { new String("data").getBytes("UTF-8") }
        request.getInputStream() >> { is }
        request.getPathInfo() >> { "/dataset/identifier" }
        request.getMethod() >> { "PUT" }
        request.getContentType() >> { "text/plain" }
        storage.store(_) >> { throw new Exception("This shouldn't happen") }
        when:
        crud.doPost(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_FORBIDDEN

    }


    def "should update document"() {
        given:
        def is = GroovyMock(ServletInputStream.class)
        is.getBytes() >> { mapper.writeValueAsBytes(["@id":"/dataset/identifier","@type":"Record","contains":"some new data"]) }
        request.getInputStream() >> { is }
        request.getPathInfo() >> { "/dataset/identifier" }
        request.getMethod() >> { "PUT" }
        request.getContentType() >> { "appplication/ld+json" }
        request.getAttribute(_) >> {
            if (it.first() == "user") {
                return ["user":"SYSTEM"]
            }
        }
        request.getRequestURL() >> { return new StringBuffer("/dataset/identifier") }
        storage.load(_) >> {
            new Document(
                    it.first(),
                    ["@id":it.first(),"@type":"Record","contains":"some data"],
                    ["dataset":"dataset","modified":new Date().getTime(), "created":new Date(0).getTime()]
            )
        }
        storage.store(_) >> { return it.first() }
        when:
        crud.doPost(request, response)
        then:
        response.getStatus() == HttpServletResponse.SC_CREATED


    }

    def "should create new document"() {

    }

    def "should delete document"() {

    }

    def "should calculate dataset based on path"() {
        expect:
        crud.getDatasetBasedOnPath("/bib/12345") == "bib"
        crud.getDatasetBasedOnPath("/dataset") == "dataset"
        crud.getDatasetBasedOnPath("/dataset/") == "dataset"
        crud.getDatasetBasedOnPath("/sys/common/foobar") == "sys"
        crud.getDatasetBasedOnPath("/") == ""

    }

}
