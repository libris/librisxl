package whelk.rest.api

import spock.lang.Specification
import spock.lang.Unroll

import static whelk.JsonLd.ABOUT_KEY

import javax.servlet.ServletInputStream
import javax.servlet.ServletOutputStream
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import javax.servlet.http.HttpServletResponseWrapper
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST
import static javax.servlet.http.HttpServletResponse.SC_NOT_ACCEPTABLE
import static javax.servlet.http.HttpServletResponse.SC_OK


@Unroll
class ConverterAPISpec extends Specification {

    ConverterAPI converterApi

    HttpServletRequest request
    HttpServletResponse response

    def converterUrl = "http://localhost:8180/_convert"

    void setup() {
        request = GroovyMock(HttpServletRequest)
        def out = GroovyMock(ServletOutputStream)
        response = new HttpServletResponseWrapper(GroovyMock(HttpServletResponse)) {
            int status = 0
            String contentType
            def headers = [:]
            ServletOutputStream getOutputStream() { out }
            void sendError(int code, String msg) { this.status = code }
            void setHeader(String h, String v) { headers.put(h, v) }
            String getHeader(String h) { headers.get(h) }
        }
        converterApi = new ConverterAPI()
    }

    def "should convert data"() {
        given:
        def bytes = input.getBytes("UTF-8")
        def is = GroovyMock(ServletInputStream)
        is.getBytes() >> { bytes }
        is.getText("UTF-8") >> { input }
        request.getMethod() >> { "POST" }
        request.getRequestURL() >> { converterUrl }
        //request.getParameter("to") >> accept
        request.getHeader("Accept") >> accept
        request.getInputStream() >> { is }
        request.getContentLength() >> {  bytes.size() }
        request.getContentType() >> { contentType }
        when:
        converterApi.doPost(request, response)
        then:
        response.status == statusCode
        response.contentType == (response.status == SC_OK? accept : null)
        where:
        contentType                         | input | accept       | statusCode
        'application/ld+json'               | INPUT | XMARCJSON    | SC_OK
        'application/x-www-form-urlencoded' | INPUT | XMARCJSON    | SC_BAD_REQUEST
        'application/ld+json'               | ''    | XMARCJSON    | SC_BAD_REQUEST
        'application/ld+json'               | INPUT | 'text/html'  | SC_NOT_ACCEPTABLE
    }

    static XMARCJSON = 'application/x-marc-json'
    static INPUT = """
        {
          "@graph": [
            {
              "@id": "http://libris.kb.se/bib/0000000",
              "$ABOUT_KEY": {"@id": "http://127.0.0.1:5000/000000000000000#it"}
            },
            {
              "@id": "http://127.0.0.1:5000/000000000000000#it",
              "instanceOf": {"@type": "Text"}
            }
          ]
        }
    """

}
