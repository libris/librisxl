package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.component.ElasticJsonMapper
import spock.lang.*


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
        def creator = ["@type": "Person", "controlledLabel": "Jansson, Tove, 1914-2001"]
        def contributorList = [["@type": "Person", "controlledLabel": "Jansson, Tove, 1914-2001"], ["@type": "Person", "controlledLabel": "Jansson, Tove, 1914-2001"]]
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
                "@id": "/auth/12345",
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
            doclist.size() == 2
            doclist[0].dataAsMap.about."@id" == "/resource/bib/7149593"
            doclist[1].dataAsMap."@id" == "/bib/Person/Jansson%2C+Tove%2C+1914-2001"

        when:
            copy = bib
            copy["about"]["instanceOf"]["creator"] = creator
            copy["about"]["instanceOf"]["contributorList"] = contributorList
            doclist = converter.doConvert(makeDoc(copy, "http://libris.kb.se/bib/7149593"))
        then:
            doclist.size() == 4
            doclist[0].dataAsMap.about."@id" == "/resource/bib/7149593"
            doclist[1].dataAsMap."@id" == "/bib/Person/Jansson%2C+Tove%2C+1914-2001"
            doclist[2].dataAsMap."@id" == "/bib/Person/Jansson%2C+Tove%2C+1914-2001"
            doclist[3].dataAsMap."@id" == "/bib/Person/Jansson%2C+Tove%2C+1914-2001"

        when:
            doclist = converter.doConvert(makeDoc(authPerson, "http://libris.kb.se/auth/1234"))
        then:
            doclist.size() == 2
            doclist[0].dataAsMap.about."@id" == "/resource/auth/191503"
            //doclist[1].dataAsMap."@id" == "/resource/auth/191503"

        when:
            doclist = converter.doConvert(makeDoc(authConcept, "http://libris.kb.se/auth/1234"))
        then:
            doclist.size() == 2
            doclist[0].dataAsMap."@id" == "/auth/12345"
            doclist[1].dataAsMap."@id" == "/resource/auth/12345"

        when:
            doclist = converter.doConvert(makeDoc(conceptScheme, "http://libris.kb.se/auth/1234"))
        then:
            doclist.size() == 2
            doclist[0].dataAsMap."@id" == "/auth/12345"
            doclist[1].dataAsMap."@id" == "/auth/ConceptScheme/barn"

    }

    def makeDoc(data, id) {
        return new Document()
            .withIdentifier(id)
            .withData(mapper.writeValueAsBytes(data))
    }

}
