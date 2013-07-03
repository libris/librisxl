package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.IndexDocument
import se.kb.libris.whelks.component.ElasticJsonMapper
import spock.lang.*


class JsonLDEntityExtractorIndexFormatConverterSpec extends Specification {

    def converter = new JsonLDEntityExtractorIndexFormatConverter()
    private mapper = new ElasticJsonMapper()

    def "convert should create multiple entities of expected types"() {
        given:
        def source = [
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
        def contributorList = [
            ["@type": "Person", "controlledLabel": "Pietil\u00e4, Tuulikki, 1917-"]
        ]
        def subject = [
            ["@type": "Concept", "prefLabel": "Litteraturhistoria Finlandssvensk"]
        ]
        def doclist = null
        def copy = null

        when:
        doclist = converter.doConvert(makeDoc(source))
        then:
        doclist.size() == 3

        when:
        copy = source.clone()
        copy.about.instanceOf.creator = creator
        doclist = converter.doConvert(makeDoc(copy))
        then:
        doclist.size() == 4

        when:
        copy = source.clone()
        copy.about.instanceOf.creator = creator
        copy.about.instanceOf.contributorList = contributorList
        copy.about.instanceOf.subject = subject
        doclist = converter.doConvert(makeDoc(copy))
        then:
        doclist.size() == 6

    }

    def makeDoc(data) {
        // TODO: this serialization-deserialization seems redundant
        return new IndexDocument()
            .withIdentifier("http://libriks.kb.se/bib/7149593")
            .withData(mapper.writeValueAsString(data))
    }

}
