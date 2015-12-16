package whelk.filter

import spock.lang.Specification
import whelk.Document
import whelk.Location
import whelk.component.PostgreSQLComponent

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

/**
 * Created by markus on 2015-12-16.
 */
class LinkFinderSpec extends Specification {

    LinkFinder linkFinder

    void setup() {
        PostgreSQLComponent storage = GroovyMock(PostgreSQLComponent.class)
        Connection pgsqlConnection = GroovyMock(Connection.class)
        PreparedStatement stmt = GroovyMock(PreparedStatement.class)
        ResultSet result = GroovyMock(ResultSet.class)
        result.next() >> { true }
        result.getString(_) >> { "replacedValue" }
        stmt.executeQuery() >> { result }
        pgsqlConnection.prepareStatement(_) >> { stmt }
        storage.getConnection() >> { pgsqlConnection }

        storage.locate(_,_) >> { new Location(new Document(it.first(), ["@id":"/bib/1234", "foo":"bar"], ["dataset":"bib", (Document.CONTENT_TYPE_KEY):"application/ld+json", "identifier":it.first(), (Document.MODIFIED_KEY): new Date().getTime()])) }
        linkFinder = new LinkFinder(storage)
    }

    def DOCUMENT_DATA = [
            "descriptions": [
                    "entry": [
                            "@id":"/foobar",
                            "createdBy": ["@id":"/some?type=Something"],
                            "name":"Not important"
                    ],
                    "items":[
                            [
                                    "@id":"/foobar#it",
                                    "attributedTo":["@id":"/some?type=Person"]
                            ],
                            [
                                    "data":"crap",
                                    "someEvent":[
                                            [
                                                    "eventName":"groggo",
                                                    "@id": "/some?type=Event&eventName=groggo"
                                            ]
                                    ]
                            ]

                    ]
            ]
    ]

    def "should replace all /some?-identifiers"() {
        given:
        Document unfilteredDocument = new Document("foobar", DOCUMENT_DATA).withContentType("application/ld+json")
        Document filteredDocument = linkFinder.findLinks(unfilteredDocument)
        expect:
        filteredDocument.data.descriptions.entry.createdBy["@id"] == "replacedValue"
        filteredDocument.data.descriptions.items[0].attributedTo["@id"] == "replacedValue"
        filteredDocument.data.descriptions.items[1].someEvent[0]["@id"] == "replacedValue"
        filteredDocument.data.descriptions.items[1].someEvent[0]["eventName"] == "groggo"

    }

}
