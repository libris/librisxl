package whelk.component

import groovy.util.logging.Slf4j as Log
import com.fasterxml.jackson.databind.ObjectMapper
import spock.lang.Specification
import whelk.Document
import whelk.Link

import javax.sql.DataSource
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
    def additionalDataSource = GroovyMock(DataSource)

    static private final ObjectMapper mapper = new ObjectMapper()

    static String documentData = mapper.writeValueAsString("@graph": [["@id": "testid", "name": "foobar", "sameAs": [["@id": "https://libris.kb.se/testid"]]]])
    static Object identifiers = "http://example.org/record"

    def setup() {
        conn.prepareStatement(_) >> { stmt }
        stmt.executeQuery() >> { result }
        storage = new PostgreSQLComponent((String) null) {
            @Override
            Connection getMyConnection() {
                log.info("Getting connection ...")
                conn
            }

            @Override
            List<String> followDependers(String id) {
                return []
            }

            @Override
            DataSource createAdditionalConnectionPool(String name, int size) {
                additionalDataSource
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
        String cs1 = new Document(["@graph": [["key": "some data", "@id": "testid", "created": 1298619387, "modified": 10284701387], ["identifier": "testid", "collection": "test"]]]).getChecksum(null)
        String cs2 = new Document(["@graph": [["@id": "testid", "key": "some data", "created": 1298619287, "modified": 10284701287], ["identifier": "testid", "collection": "test"]]]).getChecksum(null)
        String cs3 = new Document(["@graph": [["@id": "testid", "key": "some new data", "created": 1298619287, "modified": 1298461982639], ["identifier": "testid", "collection": "test"]]]).getChecksum(null)
        String cs4 = new Document(["@graph": [["@id": "testid", "key": "some data", "created": 1298619387, "modified": 10284701387], ["identifier": "testid", "collection": "test"]]]).getChecksum(null)

        then:
        cs4 == cs2
        cs2 != cs3
        cs1 == cs2

    }

    def "should calculate different checksums when a list is reordered"() {
        when:
        String cs1 = new Document(["@graph": [["key": "some data", "@id": "testid"], ["identifier": "testid", "collection": "test"]]]).getChecksum(null)
        String cs2 = new Document(["@graph": [["identifier": "testid", "collection": "test"], ["@id": "testid", "key": "some data"]]]).getChecksum(null)

        then:
        cs1 != cs2
    }

    def "should calculate equal checksums when objects in an object change order"() {
        when:
        String cs1 = new Document(["@graph": [["key": "some data", "@id": "testid"], ["identifier": "testid", "collection": "test"]]]).getChecksum(null)
        String cs2 = new Document(["@graph": [["@id": "testid", "key": "some data"], ["identifier": "testid", "collection": "test"]]]).getChecksum(null)

        then:
        cs1 == cs2
    }

    def "dependencies are links to existing documents, except to itself"() {
        given:
        Set<Link> links = [
                Link.of('instanceOf.genreForm', 'https://id.kb.se/term/a'),
                Link.of('instanceOf.subject', 'https://id.kb.se/term/a'),
                Link.of('instanceOf.language', 'https://id.kb.se/language/eng'),   // two IRIs of
                Link.of('instanceOf.language', 'https://libris.kb.se/eng-record'), // the same document
                Link.of('isPartOf', 'https://id.kb.se/does-not-exist'),
                Link.of('sameAs', 'https://libris.kb.se/self#it'),
                Link.of('note', 'urn:not-http'),
        ] as Set
        Map<String, Set<String>> liveSystemIdsByIri = [
                'https://id.kb.se/term/a'          : ['a'] as Set,
                'https://id.kb.se/language/eng'    : ['eng'] as Set,
                'https://libris.kb.se/eng-record'  : ['eng'] as Set,
                'https://libris.kb.se/self#it'     : ['self'] as Set,
                'urn:not-http'                     : ['other'] as Set,
        ]

        expect:
        PostgreSQLComponent.calculateDependencies('self', links, liveSystemIdsByIri).collect { it as List }.sort() == [
                ['instanceOf.genreForm', 'a'],
                ['instanceOf.language', 'eng'],
                ['instanceOf.language', 'eng'],
                ['instanceOf.subject', 'a'],
        ]
        PostgreSQLComponent.dependencyIris(links) == links.collect { it.iri }.findAll { it.startsWith('http') } as Set
    }

    def "changed dependencies are compared as multisets"() {
        expect:
        PostgreSQLComponent.changedDependencies(rows(expected), rows(stored)) == (changed as Set)

        where:
        expected                    | stored                      || changed
        []                          | []                          || null
        [['r', 'a'], ['s', 'b']]    | [['s', 'b'], ['r', 'a']]    || null
        [['r', 'a'], ['s', 'b']]    | [['r', 'a']]                || ['b']
        [['r', 'a']]                | [['r', 'a'], ['s', 'b']]    || ['b']
        [['r', 'a']]                | [['s', 'a']]                || ['a']
        [['r', 'a'], ['r', 'a']]    | [['r', 'a']]                || ['a']
        []                          | [['r', 'a'], ['s', 'b']]    || ['a', 'b']
    }

    private static List<String[]> rows(List<List<String>> rows) {
        return rows.collect { it as String[] }
    }

}
