package whelk.component

import groovy.util.logging.Slf4j as Log
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
            List<String> getDependers(String id) {
                return []
            }
        }
    }

    def "should save document to database"() {
        given:
        result.next() >> { true }
        result.getTimestamp(_) >> {
            return new Timestamp(new Date().getTime())
        }
        result.getString("id") >> { return "hej" }
        stmt.executeUpdate() >> { 1 }

        Document doc = null
        when:
        doc = new Document(["@graph": DocumentSpec.examples.first().data])
        then:
        doc.checksum != null
        doc.id == "http://example.org/record"
        //doc.collection == "test" //no collection property anymore
        doc.created == null
        doc.modified == null
        and:
        storage.store(doc, true, null, null, "", false)
        then:
        doc.created != null
        doc.modified != null
        // r.collection == "test" //no collection property anymore
        doc.id == "http://example.org/record"
        doc.checksum != null
    }


    def "should load document from database"() {
        given:
        2 * result.next() >> { true }
        1 * result.next() >> { false }
        2 * result.next() >> { true }
        1 * result.next() >> { false }
        result.getString(_) >> {
            if (it.first() == "id") {
                return "testid"
            }
            if (it.first() == "data") {
                return documentData
            }
            if (it.first() == "iri") {
                return identifiers
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
        //r.collection == "test" //missing properties
        //r.deleted == false //missing properties
    }

    def "should return null for non existing identifier"() {
        given:
        result.next() >> { false }
        when:
        Document r = storage.load("nonexistingid")
        then:
        r == null
    }

    def "should generate correct jsonb query according to storage type"() {
        expect:
        storage.translateToSql(key, value, storageType) == [sqlKey, sqlValue]
        where:
        key                              | value                | storageType                               | sqlKey                               | sqlValue
        "entry.@id"                      | "/bib/12345"         | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'entry' @> ?" | "{\"@id\":\"/bib/12345\"}"
        "@id"                            | "/resource/auth/345" | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'items' @> ?" | "[{\"@id\":\"/resource/auth/345\"}]"
        "items.title"                    | "Kalldrag"           | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'items' @> ?" | "[{\"title\":\"Kalldrag\"}]"
        "items.instanceTitle.titleValue" | "Kalldrag"           | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'items' @> ?" | "[{\"instanceTitle\":{\"titleValue\":\"Kalldrag\"}}]"
        "instanceTitle.titleValue"       | "Kalldrag"           | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'items' @> ?" | "[{\"instanceTitle\":{\"titleValue\":\"Kalldrag\"}}]"
        "245.a"                          | "Kalldrag"           | StorageType.MARC21_JSON                   | "data->'fields' @> ?"                | "[{\"245\":{\"subfields\":[{\"a\":\"Kalldrag\"}]}}]"
        "024.a"                          | "d12345"             | StorageType.MARC21_JSON                   | "data->'fields' @> ?"                | "[{\"024\":{\"subfields\":[{\"a\":\"d12345\"}]}}]"
    }

    def "should generate correct ORDER BY according to storage type"() {
        expect:
        storage.translateSort(keys, storageType) == orderBy
        where:
        keys                      | storageType                               | orderBy
        "245.a"                   | StorageType.MARC21_JSON                   | "data->'fields'->'245'->'subfields'->'a' ASC"
        "245.a,024.a"             | StorageType.MARC21_JSON                   | "data->'fields'->'245'->'subfields'->'a' ASC, data->'fields'->'024'->'subfields'->'a' ASC"
        "245.a,-024.a"            | StorageType.MARC21_JSON                   | "data->'fields'->'245'->'subfields'->'a' ASC, data->'fields'->'024'->'subfields'->'a' DESC"
        "entry.title,entry.-isbn" | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'entry'->'title' ASC, data->'descriptions'->'entry'->'isbn' DESC"
        "items.title,items.-isbn" | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'items'->'title' ASC, data->'descriptions'->'items'->'isbn' DESC"
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
