package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.component.ElasticJsonMapper
import spock.lang.*
import groovy.util.logging.Slf4j as Log

@Log
class JsonLDEntityExtractorIndexFormatConverterSpec extends Specification {

    def converter = new JsonLDEntityExtractorIndexFormatConverter()
    private mapper = new ElasticJsonMapper()

    def "convert should create multiple entities of expected types"() {
        given:
        def bib = [
            "@id": "/bib/7149593",
            "@type": "Record",
            "about": [
                "@id": "/resource/bib/7149593",
                "@type": "Book",
                "instanceOf": [
                    "@type": "Book",
                ]
            ]
        ]
        def creator = ["@type": "Person", "controlledLabel": "Jansson, Tove, 1914-2001", "@id": "/resource/auth/191503"]
        def creator2 = ["@type": "Person", "controlledLabel": "Svensson, Unauthorized"]
        def contributorList = [["@type": "Person", "controlledLabel": "Jansson, Tove, 1914-2001", "@id": "/resource/auth/191503"], ["@type": "Person", "controlledLabel": "Hopp, Hej"]]
        def authPerson = [
                "@id": "/auth/191503",
                "@type": "Record",
                "about": [
                        "@id": "/resource/auth/191503",
                        "@type": "Person",
                        "controlledLabel": "Jansson, Tove, 1914-2001",
                        "sameAs": ["@id": "http://viaf.org/viaf/111533709"]
                ]
        ]
        def authConcept =  [
                "@id": "/auth/12345",
                "@type": "Record",
                "about": [
                        "@id": "/topic/sao/historia",
                        "@type": "Concept",
                        "prefLabel": "Historia",
                        "sameAs": ["@id": "/resource/auth/12345"]
                ]
        ]
        def conceptScheme =  [
                "@id": "/auth/123456",
                "@type": "Record",
                "about": ["inScheme": ["@type": "ConceptScheme", "@id": "/topic/barn", "notation": "barn"]]
        ]
        def doclist = null
        def copy = null

        when:
            doclist = converter.doConvert(makeDoc(bib, "http://libris.kb.se/bib/7149593"))
        then:
            doclist.size() == 1
            doclist[0].dataAsMap.about."@id" == "/resource/bib/7149593"

        when:
            copy = bib
            copy["about"]["instanceOf"]["creator"] = creator
            doclist = converter.doConvert(makeDoc(copy, "http://libris.kb.se/bib/7149593"))
        then:
            doclist.size() == 1
            doclist[0].dataAsMap.about."@id" == "/resource/bib/7149593"

        when:
            copy = bib
            copy["about"]["instanceOf"]["creator"] = creator2
            copy["about"]["instanceOf"]["contributorList"] = contributorList
            doclist = converter.doConvert(makeDoc(copy, "http://libris.kb.se/bib/7149593"))
        then:
            doclist.size() == 3
            doclist[0].dataAsMap.about."@id" == "/resource/bib/7149593"
            doclist[1].dataAsMap."@id" == "/bib/Person/Svensson%2C+Unauthorized"
            doclist[2].dataAsMap."@id" == "/bib/Person/Hopp%2C+Hej"

        when:
            doclist = converter.doConvert(makeDoc(authPerson, "http://libris.kb.se/auth/1234"))
        then:
            doclist.size() == 2
            doclist[0].dataAsMap.about."@id" == "/resource/auth/191503"
            doclist[1].dataAsMap."@id" == "/resource/auth/191503"

        when:
            doclist = converter.doConvert(makeDoc(authConcept, "http://libris.kb.se/auth/1234"))
        then:
            doclist.size() == 2
            doclist[0].dataAsMap."@id" == "/auth/12345"
            doclist[1].dataAsMap."@id" == "/topic/sao/historia"
            doclist[1].dataAsMap.sameAs."@id" == "/resource/auth/12345"

        when:
            doclist = converter.doConvert(makeDoc(conceptScheme, "http://libris.kb.se/auth/123456"))
        then:
            doclist.size() == 2
            doclist[0].dataAsMap."@id" == "/auth/123456"
            doclist[1].dataAsMap."@id" == "/auth/ConceptScheme/barn"

    }

    def makeDoc(data, id) {
        return new Document()
            .withIdentifier(id)
            .withData(mapper.writeValueAsBytes(data))
    }

}
