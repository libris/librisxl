package whelk.rest.api

import groovy.json.JsonSlurper
import spock.lang.Specification
import whelk.util.Romanizer

import javax.servlet.ServletInputStream
import javax.servlet.ServletOutputStream
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import javax.servlet.http.HttpServletResponseWrapper

import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND
import static javax.servlet.http.HttpServletResponse.SC_NO_CONTENT
import static javax.servlet.http.HttpServletResponse.SC_OK
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST

class TransliterationAPISpec extends Specification {
    TransliterationAPI transliteration

    HttpServletRequest request
    HttpServletResponse response
    JsonSlurper jsonSlurper

    void setup() {
        transliteration = new TransliterationAPI()
        transliteration.romanizer = new Romanizer()
        
        jsonSlurper = new JsonSlurper()

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
    }

    def "POST to / should return transliteration"() {
        given:
        def input = "{\"langTag\": \"${tag}\", \"source\": \"${source}\"}"
        def bytes = input.getBytes("UTF-8")
        def is = GroovyMock(ServletInputStream)
        is.getBytes() >> { bytes }
        is.getText("UTF-8") >> { input }
        request.getInputStream() >> { is }
        request.getPathInfo() >> { "/" }
        request.getMethod() >> { "POST" }
        transliteration.doPost(request, response)

        expect:
        response.getStatus() == expectedStatus
        if (expectedStatus == SC_OK) {
            Map parsedResponse = jsonSlurper.parseText(response.getResponseBody())
            parsedResponse.containsKey(expectedKey)
            parsedResponse[expectedKey] == expectedValue
        }

        where:
        tag | source | expectedKey | expectedValue | expectedStatus
        "uk"  | "Королівська бібліотека Швеції"  | "uk-Latn-t-uk-Cyrl-m0-iso-1995"   | "Korolivsʹka biblioteka Šveciï"    | SC_OK
        "grc" | "Εθνική βιβλιοθήκη της Σουηδίας" | "grc-Latn-t-grc-Grek-x0-skr-1980" | "Ethnikē bibliothēkē tēs Souēdias" | SC_OK
        "nonexistent" | "klsdfjsklfjsdf" | "" | "" | SC_BAD_REQUEST
    }


    def "POST with missing parameter(s) should return 400()"() {
        given:
        def bytes = input.getBytes("UTF-8")
        def is = GroovyMock(ServletInputStream)
        is.getBytes() >> { bytes }
        is.getText("UTF-8") >> { input }
        request.getInputStream() >> { is }
        request.getPathInfo() >> { "/" }
        request.getMethod() >> { "POST" }
        transliteration.doPost(request, response)

        expect:
        response.getStatus() == SC_BAD_REQUEST

        where:
        input << [
                '{"langTag": "uk", "sauce": "foo"}',
                '{"langTag": "uk"}',
                '{"source": "foo"}',
        ]
    }

    def "GET/HEAD /language/<language-tag> should check if language is supported"() {
        given:
        request.getPathInfo() >> { "/language/" + langTag }
        request.getMethod() >> { method }
        switch (method) {
            case "GET":
                transliteration.doGet(request, response)
                break
            case "HEAD":
                transliteration.doHead(request, response)
                break
        }

        expect:
        response.getStatus() == expectedStatus

        where:
        langTag    | method  | expectedStatus
        "be"       | "GET"   | SC_NO_CONTENT
        "grc"      | "GET"   | SC_NO_CONTENT
        "uk"       | "GET"   | SC_NO_CONTENT
        "mn-Mong"  | "GET"   | SC_NO_CONTENT
        "rom-Cyrl" | "GET"   | SC_NO_CONTENT
        "xxxxxxxx" | "GET"   | SC_NOT_FOUND
        "be"       | "HEAD"  | SC_NO_CONTENT
        "grc"      | "HEAD"  | SC_NO_CONTENT
        "uk"       | "HEAD"  | SC_NO_CONTENT
        "mn-Mong"  | "HEAD"  | SC_NO_CONTENT
        "rom-Cyrl" | "HEAD"  | SC_NO_CONTENT
        "xxxxxxxx" | "HEAD"  | SC_NOT_FOUND
    }
}
