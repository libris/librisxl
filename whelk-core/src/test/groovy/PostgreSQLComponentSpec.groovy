package whelk.component

import groovy.util.logging.Log4j2 as Log
import org.codehaus.jackson.map.ObjectMapper
import spock.lang.Specification
import whelk.Document
import whelk.DocumentSpec

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp

@Log
class PostgreSQLComponentSpec extends Specification {

    PostgreSQLComponent storage
    def stmt = GroovyMock(PreparedStatement)
    def conn = GroovyMock(Connection)
    def result = GroovyMock(ResultSet)


    static private final ObjectMapper mapper = new ObjectMapper()

    static String documentData = mapper.writeValueAsString("@graph": [["@id": "testid", "name": "foobar", "sameAs": [["@id": "https://libris.kb.se/testid"]]]])
    static Object identifiers = "http://example.org/record"

    def setup() {
        conn.prepareStatement(_) >> { stmt }
        stmt.executeQuery() >> { result }
        storage = new PostgreSQLComponent(null, "lddb") {
            @Override
            Connection getConnection() {
                log.info("Getting connection ...")
                conn
            }

            @Override
            List<String> followDependers(String id) {
                return []
            }
        }
    }

    def "should load document from database"() {
        given:
        1 * result.next() >> { true }
        result.getString(_) >> {
            if (it.first() == "id") {
                return "testid"
            }
            if (it.first() == "data") {
                return documentData
            }
        }
        result.getTimestamp(_) >> {
            return new Timestamp(new Date().getTime())
        }
        when:
        Document r = storage.load("testid")
        then:
        r.getShortId() == "testid"
        r.created != null
        r.modified != null
    }

    def "should return null for non existing identifier"() {
        given:
        result.next() >> { false }
        when:
        Document r = storage.load("nonexistingid")
        then:
        r == null
    }

    def "should calculate correct checksum regardless of created, modified or previous checksum"() {
        when:
        //String cs1 = new Document(["@graph": [["key": "some data", "@id": "testid"], ["identifier": "testid", "collection": "test", "created": 1298619287, "modified": 10284701287]]]).checksum
        String cs1 = new Document(["@graph": [["key": "some data", "@id": "testid", "created": 1298619387, "modified": 10284701387], ["identifier": "testid", "collection": "test"]]]).checksum
        String cs2 = new Document(["@graph": [["@id": "testid", "key": "some data", "created": 1298619287, "modified": 10284701287], ["identifier": "testid", "collection": "test"]]]).checksum
        String cs3 = new Document(["@graph": [["@id": "testid", "key": "some new data", "created": 1298619287, "modified": 1298461982639], ["identifier": "testid", "collection": "test"]]]).checksum
        String cs4 = new Document(["@graph": [["@id": "testid", "key": "some data", "created": 1298619387, "modified": 10284701387], ["identifier": "testid", "collection": "test"]]]).checksum

        then:
        cs4 == cs2
        cs2 != cs3
        cs1 == cs2

    }

    def "should calculate different checksums when a list is reordered"() {
        when:
        String cs1 = new Document(["@graph": [["key": "some data", "@id": "testid"], ["identifier": "testid", "collection": "test"]]]).checksum
        String cs2 = new Document(["@graph": [["identifier": "testid", "collection": "test"], ["@id": "testid", "key": "some data"]]]).checksum

        then:
        cs1 != cs2
    }

    def "should calculate equal checksums when objects in an object change order"() {
        when:
        String cs1 = new Document(["@graph": [["key": "some data", "@id": "testid"], ["identifier": "testid", "collection": "test"]]]).checksum
        String cs2 = new Document(["@graph": [["@id": "testid", "key": "some data"], ["identifier": "testid", "collection": "test"]]]).checksum

        then:
        cs1 == cs2
    }

}
