package whelk.rest.api

import spock.lang.Specification
import whelk.Whelk
import whelk.component.PostgreSQLComponent

import javax.servlet.ServletOutputStream
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import javax.servlet.http.HttpServletResponseWrapper

import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN
import static javax.servlet.http.HttpServletResponse.SC_NO_CONTENT
import static javax.servlet.http.HttpServletResponse.SC_OK
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST
import static javax.servlet.http.HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR

class UserDataAPISpec extends Specification {
    Whelk whelk
    PostgreSQLComponent storage
    UserDataAPI userdata

    HttpServletRequest request
    HttpServletResponse response

    private static final Map ALICE_ATTR = [email: 'alice@example.com']
    private static final String ALICE_HASH = "ff8d9819fc0e12bf0d24892e45987e249a28dce836a85cad60e28eaaa8c6d976"
    private static final Map BOB_ATTR = [email: 'bob@example.com']
    private static final String BOB_HASH = "5ff860bf1190596c7188ab851db691f0f3169c453936e9e1eba2f9a47f7a0018"

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
        whelk = new Whelk(storage)

        userdata = new UserDataAPI(whelk)
        userdata.init()
    }

    def "GET/PUT/DELETE /<id> should authenticate user"() {
        given:
        request.getPathInfo() >> {
            "/" + hashFromPath
        }
        request.getAttribute("user") >> {
            userAttr
        }
        request.getMethod() >> {
            method
        }
        switch (method) {
            case "GET":
                userdata.doGet(request, response)
                break
            case "PUT":
                request.getReader() >> {
                    new BufferedReader(new StringReader('{"foo": "bar"}'))
                }
                storage.storeUserData(_, _) >> {
                    true
                }
                userdata.doPut(request, response)
                break
            case "DELETE":
                userdata.doDelete(request, response)
                break
        }

        expect:
        response.getStatus() == expectedStatus

        where:
        hashFromPath | userAttr   | method    | expectedStatus
        ALICE_HASH   | ALICE_ATTR | "GET"     | SC_OK
        ALICE_HASH   | null       | "GET"     | SC_FORBIDDEN
        ALICE_HASH   | BOB_ATTR   | "GET"     | SC_FORBIDDEN
        ALICE_HASH   | ALICE_ATTR | "PUT"     | SC_OK
        ALICE_HASH   | null       | "PUT"     | SC_FORBIDDEN
        ALICE_HASH   | BOB_ATTR   | "PUT"     | SC_FORBIDDEN
        ALICE_HASH   | ALICE_ATTR | "DELETE"  | SC_NO_CONTENT
        ALICE_HASH   | null       | "DELETE"  | SC_FORBIDDEN
        ALICE_HASH   | BOB_ATTR   | "DELETE"  | SC_FORBIDDEN
    }

    def "GET /<id> should return user data as JSON"() {
        given:
        request.getPathInfo() >> {
            "/" + ALICE_HASH
        }
        request.getAttribute("user") >> {
            ALICE_ATTR
        }

        when:
        userdata.doGet(request, response)

        then:
        response.getStatus() == SC_OK
        response.getContentType() == "application/json"
        response.getResponseBody() == "{}"
    }


    def "GET /<id> should return 500 Internal Server Error if email is missing after authentication"() {
        given:
        request.getPathInfo() >> {
            "/" + ALICE_HASH
        }
        request.getAttribute("user") >> {
            ["foo": "bar"]
        }

        when:
        userdata.doGet(request, response)

        then:
        response.getStatus() == SC_INTERNAL_SERVER_ERROR
    }

    def "PUT /<id> should create or update data for given ID"() {
        given:
        request.getReader() >> {
            new BufferedReader(new StringReader('{"foo": "bar"}'))
        }
        request.getPathInfo() >> {
            "/" + ALICE_HASH
        }
        request.getAttribute("user") >> {
            ALICE_ATTR
        }
        request.getMethod() >> {
            "PUT"
        }
        storage.storeUserData(_, _) >> {
            true
        }
        userdata.doPut(request, response)

        expect:
        response.getStatus() == SC_OK

        where:
        body << [
                '{"foo": "bar"}',
                '["hej", ["hopp", ["san"]]',

        ]
    }

    def "PUT /<id> should return 421 Entity Too Large if body exceeds max size"() {
        given:
        request.getReader() >> {
            new BufferedReader(new StringReader("x" * 2000000))
        }
        request.getPathInfo() >> {
            "/" + ALICE_HASH
        }
        request.getAttribute("user") >> {
            ALICE_ATTR
        }
        request.getMethod() >> {
            "PUT"
        }

        when:
        userdata.doPut(request, response)

        then:
        response.getStatus() == SC_REQUEST_ENTITY_TOO_LARGE
    }

    def "PUT /<id> should return 400 Bad Request if request body is not valid JSON"() {
        given:
        request.getReader() >> {
            new BufferedReader(new StringReader(body))
        }
        request.getPathInfo() >> {
            "/" + ALICE_HASH
        }
        request.getAttribute("user") >> {
            ALICE_ATTR
        }
        request.getMethod() >> {
            "PUT"
        }
        userdata.doPut(request, response)

        expect:
        response.getStatus() == SC_BAD_REQUEST

        where:
        body << [
                "dkfslkdfjsdlkfjsdf",
                '{"foo": "bar',
                "['foo', 'bar',]",
                "",
        ]
    }

    def "PUT /<id> should return 500 Internal Server Error if valid data could not be saved"() {
        given:
        request.getReader() >> {
            new BufferedReader(new StringReader('{"foo": "bar"}'))
        }
        request.getPathInfo() >> {
            "/" + ALICE_HASH
        }
        request.getAttribute("user") >> {
            ALICE_ATTR
        }
        request.getMethod() >> {
            "PUT"
        }
        storage.storeUserData(_, _) >> {
            false
        }

        when:
        userdata.doPut(request, response)

        then:
        response.getStatus() == SC_INTERNAL_SERVER_ERROR
    }

    def "DELETE /<id> should return 204 No Content"() {
        request.getPathInfo() >> {
            "/" + ALICE_HASH
        }
        request.getAttribute("user") >> {
            ALICE_ATTR
        }
        request.getMethod() >> {
            "DELETE"
        }

        when:
        userdata.doDelete(request, response)

        then:
        response.getStatus() == SC_NO_CONTENT
    }
}
