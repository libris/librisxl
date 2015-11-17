package whelk.component

import org.codehaus.jackson.map.ObjectMapper
import spock.lang.Specification
import groovy.util.logging.Slf4j as Log
import whelk.Document

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

    static String documentManifest = mapper.writeValueAsString(["identifier":"testid", "dataset": "test"])
    static String documentData = mapper.writeValueAsString(["@id":"testid","name":"foobar"])

    def setup() {
        conn.prepareStatement(_) >> { stmt }
        stmt.executeQuery() >> { result }
        storage = new PostgreSQLComponent(null, "lddb") {
            @Override
            Connection getConnection() {
                println("Getting connection ...")
                conn
            }
        }
    }

    def "should save document to database"() {
        given:
        result.next() >> { true }
        result.getTimestamp(_) >> {
            return new Timestamp(new Date().getTime())
        }
        stmt.executeUpdate() >> { 1 }

        Document doc = null
        when:
        doc = new Document("hej", ["@id": "hej"]).withDataset("test")
        then:
        doc.checksum == null
        doc.id == "hej"
        doc.dataset == "test"
        doc.created == null
        doc.modified == null
        and:
        Document r = storage.store(doc)
        then:
        r.created != null
        r.modified != null
        r.dataset == "test"
        r.id == "hej"
        r.checksum != null
    }

    def "should load document from database"() {
        given:
        result.next() >> { true }
        result.getString(_) >> {
            if (it.first() == "id") {
                return "testid"
            }
            if (it.first() == "manifest") {
                return documentManifest
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
        r.id == "testid"
        r.created != null
        r.modified != null
        r.dataset == "test"
        r.deleted == false
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
        key                              | value                  | storageType                               | sqlKey                               | sqlValue
        "entry.@id"                      | "/bib/12345"           | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'entry' @> ?" | "{\"@id\":\"/bib/12345\"}"
        "@id"                            | "/resource/auth/345"   | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'items' @> ?" | "[{\"@id\":\"/resource/auth/345\"}]"
        "items.title"                    | "Kalldrag"             | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'items' @> ?" | "[{\"title\":\"Kalldrag\"}]"
        "items.instanceTitle.titleValue" | "Kalldrag"             | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'items' @> ?" | "[{\"instanceTitle\":{\"titleValue\":\"Kalldrag\"}}]"
        "instanceTitle.titleValue"       | "Kalldrag"             | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'items' @> ?" | "[{\"instanceTitle\":{\"titleValue\":\"Kalldrag\"}}]"
        "245.a"                          | "Kalldrag"             | StorageType.MARC21_JSON                   | "data->'fields' @> ?"                | "[{\"245\":{\"subfields\":[{\"a\":\"Kalldrag\"}]}}]"
        "024.a"                          | "d12345"               | StorageType.MARC21_JSON                   | "data->'fields' @> ?"                | "[{\"024\":{\"subfields\":[{\"a\":\"d12345\"}]}}]"
    }

    def "should generate correct ORDER BY according to storage type"() {
        expect:
        storage.translateSort(keys, storageType) == orderBy
        where:
        keys                       | storageType                               | orderBy
        "245.a"                    | StorageType.MARC21_JSON                   | "data->'fields'->'245'->'subfields'->'a' ASC"
        "245.a,024.a"              | StorageType.MARC21_JSON                   | "data->'fields'->'245'->'subfields'->'a' ASC, data->'fields'->'024'->'subfields'->'a' ASC"
        "245.a,-024.a"             | StorageType.MARC21_JSON                   | "data->'fields'->'245'->'subfields'->'a' ASC, data->'fields'->'024'->'subfields'->'a' DESC"
        "entry.title,entry.-isbn"  | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'entry'->'title' ASC, data->'descriptions'->'entry'->'isbn' DESC"
        "items.title,items.-isbn"  | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'items'->'title' ASC, data->'descriptions'->'items'->'isbn' DESC"
    }

    def "should calculate correct checksum regardless of created, modified or previous checksum"() {
        given:
        String cs
        Document doc = new Document("testid", ["key":"some data", "@id": "testid"], ["identifier": "testid", "dataset": "test", "created": 1298619287, "modified": 10284701287, "checksum": "qwudhqiuwhdiu12872"])
        and:
        doc = new Document("testid", ["@id": "testid", "key":"some data"], ["identifier": "testid", "dataset": "test", "created": 1298619287, "modified": 1298461982639, "checksum": "qwudhqiuwhdiu1287ssss2"])
        when:
        cs = storage.calculateChecksum(doc)
        then:
        cs == "52bfececee8e10e1ac8c19549b96811d"
        and:
        cs == "52bfececee8e10e1ac8c19549b96811d"

    }


}
