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

    def DOCUMENT_DATA1 = [
        "@graph": [
            [
                "@id": Document.BASE_URI.resolve("/foobar").toString(),
                "createdBy": ["@id":"/some?type=Something"],
                "name":"Not important"
            ],
            [
                "@id": Document.BASE_URI.resolve("/foobar").toString() + "#it",
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

    def DOCUMENT_DATA2 = [
            "@graph": [
                    [
                            "@id": Document.BASE_URI.resolve("/foobar").toString(),
                            "createdBy": ["@id":"/barfoo"],
                            "name":"Not important"
                    ],
                    [
                            "@id": Document.BASE_URI.resolve("/foobar").toString() + "#it",
                            "attributedTo":["@id":"/barfoo2"]
                    ],
                    [
                            "data":"crap",
                            "someEvent":[
                                    [
                                            "eventName":"groggo",
                                            "@id": "/the/groggo/event"
                                    ]
                            ]
                    ]
            ]
    ]


    def "should replace all /some?-identifiers"() {
        given:
        Document unfilteredDocument = new Document( Document.BASE_URI.resolve("/foobar").toString(), DOCUMENT_DATA1).withContentType("application/ld+json")
        Document filteredDocument = linkFinder.findLinks(unfilteredDocument)
        expect:
        filteredDocument != null
        filteredDocument.data["@graph"][0].createdBy["@id"] == "replacedValue"
        filteredDocument.data["@graph"][1].attributedTo["@id"] == "replacedValue"
        filteredDocument.data["@graph"][2].someEvent[0]["@id"] == "replacedValue"
        filteredDocument.data["@graph"][2].someEvent[0]["eventName"] == "groggo"
    }

    def "should return null for unchanged document"() {
        given:
        Document unfilteredDocument = new Document("foobar", DOCUMENT_DATA2).withContentType("application/ld+json")
        Document filteredDocument = linkFinder.findLinks(unfilteredDocument)
        expect:
        filteredDocument == null
    }

}
