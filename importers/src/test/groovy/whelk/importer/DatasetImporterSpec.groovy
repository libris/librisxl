package whelk.importer

import spock.lang.Specification

import whelk.converter.TrigToJsonLdParser

class DatasetImporterSpec extends Specification {

    def "load self compacted turtle"() {
        given:
        var s = """
        prefix : <https://id.kb.se/vocab/>
        <x> a :Record .
        """
        var data = DatasetImporter.loadSelfCompactedTurtle(new ByteArrayInputStream(s.getBytes('utf-8')))

        expect:
        data == [
            "@id": "x",
            "@type": "Record"
        ]
    }

    def "check system-shaped turtle"() {
        when:
        var s = """
        prefix : <https://id.kb.se/vocab/>
        prefix xsd: <http://www.w3.org/2001/XMLSchema#>

        <ds/1> a :Dataset ;
            :uri "ds/1" ;  # NOTE: plain string form (notice difference in JSON-LD)
            :created "2026-10-01T01:00:00Z"^^xsd:dateTime .
        <ds/2> a :Dataset ;
            :uri "ds/2"^^xsd:anyURI ;
            :created "2026-10-01T02:00:00Z"^^xsd:dateTime .
        """
        var data = TrigToJsonLdParser.parse(new ByteArrayInputStream(s.getBytes('utf-8')))

        then:
        data == [
            "@context": [
                "@vocab": "https://id.kb.se/vocab/",
                "xsd": "http://www.w3.org/2001/XMLSchema#"
            ],
            "@graph": [
                [
                    "@id": "ds/1",
                    "@type": "Dataset",
                    "uri": "ds/1" ,
                    "created": ["@type": "xsd:dateTime", "@value": "2026-10-01T01:00:00Z"]
                ],
                [
                    "@id": "ds/2",
                    "@type": "Dataset",
                    "uri": ["@type": "xsd:anyURI", "@value": "ds/2"],
                    "created": ["@type": "xsd:dateTime", "@value": "2026-10-01T02:00:00Z"]
                ]
            ]
        ]

        and:
        DatasetImporter.checkedSystemShaped(data, "https://id.kb.se/vocab/")

        and:
        data["@context"] == [
            "@vocab": "https://id.kb.se/vocab/",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "uri": ["@type": "xsd:anyURI"]
        ]
    }

}
