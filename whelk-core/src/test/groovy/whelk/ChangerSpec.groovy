package whelk

import org.apache.jena.sparql.algebra.Op
import spock.lang.Specification

class ChangerSpec extends Specification {
    def doc = exampleDoc()
    def sigel = "Sigel"

    def "changedBy sigel"() {
        given:
        def changer = Changer.sigel(sigel)

        expect:
        changer.getChangedBy() == sigel
    }

    def "changedBy unknown"() {
        given:
        def changer = Changer.unknown()

        expect:
        changer.getChangedBy() == null
    }

    def "changedBy global change"() {
        given:
        def scriptUri = 'http://thescript'
        def changer = Changer.globalChange(scriptUri, Optional.empty())

        expect:
        changer.getChangedBy() == scriptUri
    }

    def "should set last modifier"() {
        given:
        def changer = Changer.sigel(sigel)

        when:
        def minorUpdate = false
        changer.stampModified(doc, minorUpdate, new Date())

        then:
        doc.getDescriptionLastModifier() == Changer.LIBRARY_PREFIX + sigel
    }

    def "minor update should not set last modifier"() {
        given:
        def changer = Changer.sigel(sigel)
        def oldModifier = doc.getDescriptionLastModifier()

        when:
        def minorUpdate = true
        changer.stampModified(doc, minorUpdate, new Date())

        then:
        doc.getDescriptionLastModifier() == oldModifier
    }

    def "unknown should not set last modifier"() {
        given:
        def changer = Changer.unknown()
        def oldModifier = doc.getDescriptionLastModifier()

        when:
        def minorUpdate = false
        changer.stampModified(doc, minorUpdate, new Date())

        then:
        doc.getDescriptionLastModifier() == oldModifier
    }

    def "global change should always set generationProcess"() {
        given:
        def scriptUri = 'http://thescript'
        def changer = Changer.globalChange(scriptUri, Optional.empty())
        def oldDate = doc.getGenerationDate()

        when:
        changer.stampModified(doc, minorUpdate, new Date())

        then:
        doc.getGenerationProcess() == scriptUri
        doc.getGenerationDate() != oldDate

        where:
        minorUpdate << [true, false]
    }

    def "Should only set descriptionLastModifier when not minor update and sigel is available"() {
        when:
        changer.stampModified(doc, minorUpdate, new Date())

        then:
        doc.getDescriptionLastModifier() == descriptionLastModifier

        where:
        changer                                         | minorUpdate || descriptionLastModifier
        Changer.sigel("Sigel")                          | false       || Changer.LIBRARY_PREFIX + "Sigel"
        Changer.sigel("Sigel")                          | true        || Changer.LIBRARY_PREFIX + "OldSigel"
        Changer.unknown()                               | false       || Changer.LIBRARY_PREFIX + "OldSigel"
        Changer.unknown()                               | true        || Changer.LIBRARY_PREFIX + "OldSigel"
        Changer.globalChange('uri', Optional.of('Sik')) | false       || Changer.LIBRARY_PREFIX + "Sik"
        Changer.globalChange('uri', Optional.of('Sik')) | true        || Changer.LIBRARY_PREFIX + "OldSigel"
        Changer.globalChange('uri', Optional.empty())   | false       || Changer.LIBRARY_PREFIX + "OldSigel"
        Changer.globalChange('uri', Optional.empty())   | true        || Changer.LIBRARY_PREFIX + "OldSigel"
        Changer.sigelOrUnknown(null)                    | false       || Changer.LIBRARY_PREFIX + "OldSigel"
        Changer.sigelOrUnknown(null)                    | true        || Changer.LIBRARY_PREFIX + "OldSigel"
        Changer.sigelOrUnknown("Sigel")                 | false       || Changer.LIBRARY_PREFIX + "Sigel"
        Changer.sigelOrUnknown("Sigel")                 | true        || Changer.LIBRARY_PREFIX + "OldSigel"
    }

    static Document exampleDoc() {
        def graph = [
                [
                        "@id": "http://example.org/record",
                        "@type": "Record",
                        "sameAs": [["@id": "https://libris.kb.se/fnrblrghr1234567"]],
                        "mainEntity": ["@id": "http://example.org/thing"],
                        "descriptionLastModifier": ["@id" : Changer.LIBRARY_PREFIX + "OldSigel"],
                        "generationProcess": ["@id" : "http://id.kb.se/generator/globalchanges"],
                        "generationDate": "2018-09-13T02:21:01.744+02:00"
                ],
                [
                        "@id": "http://example.org/thing",
                        "@type": "Thing",
                        "sameAs": [["@id": "https://libris.kb.se/fnrblrghr1234567#it"]]
                ]
        ]

        return new Document(['@graph': graph])
    }
}
