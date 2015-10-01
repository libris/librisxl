package whelk.component

import spock.lang.Specification
import groovy.util.logging.Slf4j as Log
import whelk.Document

import java.sql.Connection
import java.sql.PreparedStatement

@Log
class PostgreSQLComponentSpec extends Specification {

    Storage storage


    def "should save document to database"() {
        given:
        Document doc = new Document("hej", ["@id": "hej"]).withDataset("test")
        when:
        Document r = storage.store(doc)
        then:
        r.created != null
    }


    def setup() {
        def stmt = GroovyMock(PreparedStatement)
        def conn = GroovyMock(Connection)

        conn.prepareStatement(_) >> { stmt }

        storage = new PostgreSQLComponent(null, "lddb") {
            @Override
            Connection getConnection() {
                println("Getting connection ...")
                conn
            }
        }
    }
}
