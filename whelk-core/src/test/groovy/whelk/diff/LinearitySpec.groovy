package whelk.diff

import spock.lang.Specification
import static whelk.util.Jackson.mapper

class LinearitySpec extends Specification {

    def "linearity 1"() {
        given:

        def versions = [
                [
                        "a":"b"
                ],
                [
                        "b":"c"
                ],
                [
                        "c":"d"
                ]
        ]

        // Common for all tests:
        def diffs = []
        for (int i = 0; i < versions.size()-1; ++i) {
            diffs.add( Diff.diff(versions[i], versions[i+1]) )
        }
        def recreatedVersions = [versions[0]]
        for (int i = 0; i < diffs.size(); ++i) {
            //System.err.println(mapper.writeValueAsString(diffs[i]))
            recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
        }
        expect:
        recreatedVersions.equals(versions)
    }

    def "linearity 2"() {
        given:

        def versions = [
                [
                        "a":["b", "c"]
                ],
                [
                        "a":["b", "c", "d"]
                ],
                [
                        "a":["b", "c", "d"], "e":["f"]
                ]
        ]

        // Common for all tests:
        def diffs = []
        for (int i = 0; i < versions.size()-1; ++i) {
            diffs.add( Diff.diff(versions[i], versions[i+1]) )
        }
        def recreatedVersions = [versions[0]]
        for (int i = 0; i < diffs.size(); ++i) {
            recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
        }
        expect:
        recreatedVersions.equals(versions)
    }

    def "linearity 3"() {
        given:

        def versions = [
                [
                        "a":["b", "c"]
                ],
                [
                        "a":["b", "c", "d"]
                ],
                [
                        "a":["b", "c"]
                ],
                [
                        "a":["b", "c", "d"], "e":["f"]
                ],
                [
                        "a":["b", "c"], "e": ["a": "b"]
                ]
        ]

        // Common for all tests:
        def diffs = []
        for (int i = 0; i < versions.size()-1; ++i) {
            diffs.add( Diff.diff(versions[i], versions[i+1]) )
        }
        def recreatedVersions = [versions[0]]
        for (int i = 0; i < diffs.size(); ++i) {
            recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
        }
        expect:
        recreatedVersions.equals(versions)
    }

    def "linearity 4"() {
        given:

        def versions = [
                [
                        "a":["b", "c"]
                ],
                [
                        "a":["b", "c", "d"]
                ],
                [
                        "a":["b", "c"]
                ]
        ]

        // Common for all tests:
        def diffs = []
        for (int i = 0; i < versions.size()-1; ++i) {
            diffs.add( Diff.diff(versions[i], versions[i+1]) )
        }
        def recreatedVersions = [versions[0]]
        for (int i = 0; i < diffs.size(); ++i) {
            recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
        }
        expect:
        recreatedVersions.equals(versions)
    }

    def "linearity 5"() {
        given:

        def versions = [
                [
                        "a":["b", "c"]
                ],
                [
                        "a":["b", "c", "d", ["a": ["b", 2]]]
                ],
                [
                        "a":["b", "c", "d", ["a": ["b", 3], "c":[true, "ok"]]]
                ],
                [
                        "a":["b", "c", "d", ["c": ["b", 3], "a":[true, "ok"]]]
                ],
        ]

        // Common for all tests:
        def diffs = []
        for (int i = 0; i < versions.size()-1; ++i) {
            diffs.add( Diff.diff(versions[i], versions[i+1]) )
        }
        def recreatedVersions = [versions[0]]
        for (int i = 0; i < diffs.size(); ++i) {
            //System.err.println(mapper.writeValueAsString(diffs[i]))
            recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
        }
        expect:
        recreatedVersions.equals(versions)
    }

    def "linearity 6"() {
        given:

        def versions = [
                [
                        "a":["b", "c", "d", "e", "f", "g"]
                ],
                [
                        "a":["e", "c", "d", "b", "f", "g", "h"]
                ],
                [
                        "a":["i", "e", "g", "d", "b", "f", "c"]
                ],
        ]

        // Common for all tests:
        def diffs = []
        for (int i = 0; i < versions.size()-1; ++i) {
            diffs.add( Diff.diff(versions[i], versions[i+1]) )
        }
        def recreatedVersions = [versions[0]]
        for (int i = 0; i < diffs.size(); ++i) {
            //System.err.println(mapper.writeValueAsString(diffs[i]))
            recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
        }
        expect:
        recreatedVersions.equals(versions)
    }

    def "linearity real record"() {
        given:

        def json_versions = [
                """{"@graph": [{"@id": "https://libris-qa.kb.se/0494flz55qzm4c5", "date": "1990-11-28T00:00:00.0+01:00", "@type": "Record", "sameAs": [{"@id": "http://libris.kb.se/hold/18318425"}], "created": "2005-03-08T15:28:12+01:00", "modified": "2016-12-08T13:53:02+01:00", "mainEntity": {"@id": "https://libris-qa.kb.se/0494flz55qzm4c5#it"}, "recordStatus": "marc:CorrectedOrRevised", "controlNumber": "18318425", "encodingLevel": "marc:HoldingsLevel4", "generationDate": "2018-06-05T21:02:57.405+02:00", "cataloguersNote": ["Kurs 18/19"], "_marcUncompleted": [{"541": {"ind1": " ", "ind2": " ", "subfields": [{"a": "Kurs: Kreativ patisserie och dessert. 15 p."}]}, "_unhandled": ["ind1"]}], "marc:holdingType": "marc:SinglePartItemHolding", "generationProcess": {"@id": "https://id.kb.se/generator/marcframe"}, "descriptionLastModifier": {"@id": "https://libris.kb.se/library/Og"}}, {"@id": "https://libris-qa.kb.se/0494flz55qzm4c5#it", "@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "itemOf": {"@id": "https://libris-qa.kb.se/r821kzk340kvpkx#it"}, "sameAs": [{"@id": "http://libris.kb.se/resource/hold/18318425"}], "hasComponent": [{"@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "shelfMark": {"@type": "ShelfMark", "label": "Kurs REF"}, "copyNumber": "1", "availability": [{"@type": "ItemAvailability", "label": "McGee"}], "physicalLocation": ["GENERAL"]}, {"@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "shelfMark": {"@type": "ShelfMark", "label": "Kurs"}, "copyNumber": "2, 3, 4, 5, 6", "availability": [{"@type": "ItemAvailability", "label": "McGee"}], "physicalLocation": ["COURSE"]}], "inventoryLevel": 1, "marc:copyReport": {"@id": "https://id.kb.se/marc/Separate"}, "marc:completeness": {"@id": "https://id.kb.se/marc/CompletenessType-1"}, "marc:lendingPolicy": {"code": "u"}, "marc:retentionPolicy": {"@id": "https://id.kb.se/marc/PermanentlyRetained"}, "marc:acquisitionMethod": {"code": "u"}, "marc:acquisitionStatus": {"@id": "https://id.kb.se/marc/CurrentlyReceived"}, "marc:reproductionPolicy": {"code": "u"}}]}"""
                ,
                """{"@graph": [{"@id": "https://libris-qa.kb.se/0494flz55qzm4c5", "date": "1990-11-28T00:00:00.0+01:00", "@type": "Record", "sameAs": [{"@id": "http://libris.kb.se/hold/18318425"}], "created": "2005-03-08T15:28:12+01:00", "modified": "2018-09-28T10:27:12.416+02:00", "mainEntity": {"@id": "https://libris-qa.kb.se/0494flz55qzm4c5#it"}, "recordStatus": "marc:CorrectedOrRevised", "controlNumber": "18318425", "encodingLevel": "marc:HoldingsLevel4", "generationDate": "2018-06-05T21:02:57.405+02:00", "cataloguersNote": ["Kurs 18/19"], "_marcUncompleted": [{"541": {"ind1": " ", "ind2": " ", "subfields": [{"a": "Kurs: Kreativ patisserie och dessert. 15 p."}]}, "_unhandled": ["ind1"]}], "marc:holdingType": "marc:SinglePartItemHolding", "generationProcess": {"@id": "https://id.kb.se/generator/marcframe"}, "descriptionLastModifier": {"@id": "https://libris.kb.se/library/Og"}}, {"@id": "https://libris-qa.kb.se/0494flz55qzm4c5#it", "@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "itemOf": {"@id": "https://libris-qa.kb.se/r821kzk340kvpkx#it"}, "sameAs": [{"@id": "http://libris.kb.se/resource/hold/18318425"}], "hasComponent": [{"@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "shelfMark": {"@type": "ShelfMark", "label": "Kurs REF"}, "copyNumber": "1", "availability": [{"@type": "ItemAvailability", "label": "McGee"}], "physicalLocation": ["GENERAL"]}, {"@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "shelfMark": {"@type": "ShelfMark", "label": "Kurs"}, "copyNumber": "2, 3, 4, 5, 6", "availability": [{"@type": "ItemAvailability", "label": "McGee"}], "physicalLocation": ["COURSE"]}], "inventoryLevel": "1", "marc:copyReport": {"@id": "https://id.kb.se/marc/Separate"}, "marc:completeness": {"@id": "https://id.kb.se/marc/CompletenessType-1"}, "marc:lendingPolicy": {"code": "u"}, "marc:retentionPolicy": {"@id": "https://id.kb.se/marc/PermanentlyRetained"}, "marc:acquisitionMethod": {"code": "u"}, "marc:acquisitionStatus": {"@id": "https://id.kb.se/marc/CurrentlyReceived"}, "marc:reproductionPolicy": {"code": "u"}}]}"""
                ,
                """{"@graph": [{"@id": "https://libris-qa.kb.se/0494flz55qzm4c5", "date": "1990-11-28T00:00:00.0+01:00", "@type": "Record", "sameAs": [{"@id": "http://libris.kb.se/hold/18318425"}], "created": "2005-03-08T15:28:12+01:00", "modified": "2018-09-28T10:27:59.139+02:00", "mainEntity": {"@id": "https://libris-qa.kb.se/0494flz55qzm4c5#it"}, "recordStatus": "marc:CorrectedOrRevised", "controlNumber": "18318425", "generationDate": "2019-10-18T19:54:37.341+02:00", "cataloguersNote": ["Kurs 18/19"], "_marcUncompleted": [{"541": {"ind1": " ", "ind2": " ", "subfields": [{"a": "Kurs: Kreativ patisserie och dessert. 15 p."}]}, "_unhandled": ["ind1"]}], "marc:holdingType": "marc:SinglePartItemHolding", "generationProcess": {"@id": "https://libris.kb.se/sys/globalchanges/2019/10/remove-encodingLevels-from-holds/script.groovy"}, "descriptionLastModifier": {"@id": "https://libris.kb.se/library/Og"}}, {"@id": "https://libris-qa.kb.se/0494flz55qzm4c5#it", "@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "itemOf": {"@id": "https://libris-qa.kb.se/r821kzk340kvpkx#it"}, "sameAs": [{"@id": "http://libris.kb.se/resource/hold/18318425"}], "hasComponent": [{"@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "shelfMark": {"@type": "ShelfMark", "label": "Kurs REF"}, "copyNumber": "1", "availability": [{"@type": "ItemAvailability", "label": "McGee"}], "physicalLocation": ["GENERAL"]}, {"@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "shelfMark": {"@type": "ShelfMark", "label": "Kurs"}, "copyNumber": "2, 3, 4, 5, 6", "availability": [{"@type": "ItemAvailability", "label": "McGee"}], "physicalLocation": ["COURSE"]}], "inventoryLevel": "1", "marc:copyReport": {"@id": "https://id.kb.se/marc/Separate"}, "marc:completeness": {"@id": "https://id.kb.se/marc/CompletenessType-1"}, "marc:lendingPolicy": {"code": "u"}, "marc:retentionPolicy": {"@id": "https://id.kb.se/marc/PermanentlyRetained"}, "marc:acquisitionMethod": {"code": "u"}, "marc:acquisitionStatus": {"@id": "https://id.kb.se/marc/CurrentlyReceived"}, "marc:reproductionPolicy": {"code": "u"}}]}"""
                ,
                """{"@graph": [{"@id": "https://libris-qa.kb.se/0494flz55qzm4c5", "date": "1990-11-28T00:00:00.0+01:00", "@type": "Record", "sameAs": [{"@id": "http://libris.kb.se/hold/18318425"}], "created": "2005-03-08T15:28:12+01:00", "modified": "2018-09-28T10:27:59.139+02:00", "mainEntity": {"@id": "https://libris-qa.kb.se/0494flz55qzm4c5#it"}, "recordStatus": "marc:CorrectedOrRevised", "controlNumber": "18318425", "generationDate": "2021-05-12T18:36:20.87+02:00", "cataloguersNote": ["Kurs 18/19"], "marc:holdingType": "marc:SinglePartItemHolding", "generationProcess": {"@id": "https://libris.kb.se/sys/globalchanges/cleanups/2021/05/lxl-3583-remove-hold-unhandled-541-583.groovy"}, "descriptionLastModifier": {"@id": "https://libris.kb.se/library/Og"}}, {"@id": "https://libris-qa.kb.se/0494flz55qzm4c5#it", "@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "itemOf": {"@id": "https://libris-qa.kb.se/r821kzk340kvpkx#it"}, "sameAs": [{"@id": "http://libris.kb.se/resource/hold/18318425"}], "hasComponent": [{"@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "shelfMark": {"@type": "ShelfMark", "label": "Kurs REF"}, "copyNumber": "1", "availability": [{"@type": "ItemAvailability", "label": "McGee"}], "physicalLocation": ["GENERAL"]}, {"@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "shelfMark": {"@type": "ShelfMark", "label": "Kurs"}, "copyNumber": "2, 3, 4, 5, 6", "availability": [{"@type": "ItemAvailability", "label": "McGee"}], "physicalLocation": ["COURSE"]}], "inventoryLevel": "1", "marc:copyReport": {"@id": "https://id.kb.se/marc/Separate"}, "marc:completeness": {"@id": "https://id.kb.se/marc/CompletenessType-1"}, "marc:lendingPolicy": {"code": "u"}, "marc:retentionPolicy": {"@id": "https://id.kb.se/marc/PermanentlyRetained"}, "marc:acquisitionMethod": {"code": "u"}, "marc:acquisitionStatus": {"@id": "https://id.kb.se/marc/CurrentlyReceived"}, "marc:reproductionPolicy": {"code": "u"}}]}"""
                ,
                """{"@graph": [{"@id": "https://libris-qa.kb.se/0494flz55qzm4c5", "date": "1990-11-28T00:00:00.0+01:00", "@type": "Record", "sameAs": [{"@id": "http://libris.kb.se/hold/18318425"}], "created": "2005-03-08T15:28:12+01:00", "modified": "2021-11-12T11:28:21.621+01:00", "mainEntity": {"@id": "https://libris-qa.kb.se/0494flz55qzm4c5#it"}, "recordStatus": "marc:CorrectedOrRevised", "controlNumber": "18318425", "generationDate": "2021-05-12T18:36:20.87+02:00", "cataloguersNote": ["Kurs 21/22"], "marc:holdingType": "marc:SinglePartItemHolding", "generationProcess": {"@id": "https://libris.kb.se/sys/globalchanges/cleanups/2021/05/lxl-3583-remove-hold-unhandled-541-583.groovy"}, "descriptionLastModifier": {"@id": "https://libris.kb.se/library/Og"}}, {"@id": "https://libris-qa.kb.se/0494flz55qzm4c5#it", "@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "itemOf": {"@id": "https://libris-qa.kb.se/r821kzk340kvpkx#it"}, "sameAs": [{"@id": "http://libris.kb.se/resource/hold/18318425"}], "hasComponent": [{"@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "shelfMark": {"@type": "ShelfMark", "label": "Kurs REF"}, "copyNumber": "1", "availability": [{"@type": "ItemAvailability", "label": "McGee"}], "physicalLocation": ["GENERAL"]}, {"@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "shelfMark": {"@type": "ShelfMark", "label": "Kurs"}, "copyNumber": "2, 3, 4, 5, 6", "availability": [{"@type": "ItemAvailability", "label": "McGee"}], "physicalLocation": ["COURSE"]}], "inventoryLevel": "1", "marc:copyReport": {"@id": "https://id.kb.se/marc/Separate"}, "marc:completeness": {"@id": "https://id.kb.se/marc/CompletenessType-1"}, "marc:lendingPolicy": {"code": "u"}, "marc:retentionPolicy": {"@id": "https://id.kb.se/marc/PermanentlyRetained"}, "marc:acquisitionMethod": {"code": "u"}, "marc:acquisitionStatus": {"@id": "https://id.kb.se/marc/CurrentlyReceived"}, "marc:reproductionPolicy": {"code": "u"}}]}"""
                ,
                """{"@graph": [{"@id": "https://libris-qa.kb.se/0494flz55qzm4c5", "date": "1990-11-28T00:00:00.0+01:00", "@type": "Record", "sameAs": [{"@id": "http://libris.kb.se/hold/18318425"}], "created": "2005-03-08T15:28:12+01:00", "modified": "2022-12-12T11:26:17.174+01:00", "mainEntity": {"@id": "https://libris-qa.kb.se/0494flz55qzm4c5#it"}, "recordStatus": "marc:CorrectedOrRevised", "controlNumber": "18318425", "generationDate": "2021-05-12T18:36:20.87+02:00", "cataloguersNote": ["Kurs 22/23"], "marc:holdingType": "marc:SinglePartItemHolding", "generationProcess": {"@id": "https://libris.kb.se/sys/globalchanges/cleanups/2021/05/lxl-3583-remove-hold-unhandled-541-583.groovy"}, "descriptionLastModifier": {"@id": "https://libris.kb.se/library/Og"}}, {"@id": "https://libris-qa.kb.se/0494flz55qzm4c5#it", "@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "itemOf": {"@id": "https://libris-qa.kb.se/r821kzk340kvpkx#it"}, "sameAs": [{"@id": "http://libris.kb.se/resource/hold/18318425"}], "hasComponent": [{"@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "shelfMark": {"@type": "ShelfMark", "label": "Kurs REF"}, "copyNumber": "1", "availability": [{"@type": "ItemAvailability", "label": "McGee"}], "physicalLocation": ["GENERAL"]}, {"@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "shelfMark": {"@type": "ShelfMark", "label": "Kurs"}, "copyNumber": "2, 3, 4, 5, 6", "availability": [{"@type": "ItemAvailability", "label": "McGee"}], "physicalLocation": ["COURSE"]}], "inventoryLevel": "1", "marc:copyReport": {"@id": "https://id.kb.se/marc/Separate"}, "marc:completeness": {"@id": "https://id.kb.se/marc/CompletenessType-1"}, "marc:lendingPolicy": {"code": "u"}, "marc:retentionPolicy": {"@id": "https://id.kb.se/marc/PermanentlyRetained"}, "marc:acquisitionMethod": {"code": "u"}, "marc:acquisitionStatus": {"@id": "https://id.kb.se/marc/CurrentlyReceived"}, "marc:reproductionPolicy": {"code": "u"}}]}"""
                ,
                """{"@graph": [{"@id": "https://libris-qa.kb.se/0494flz55qzm4c5", "date": "1990-11-28T00:00:00.0+01:00", "@type": "Record", "sameAs": [{"@id": "http://libris.kb.se/hold/18318425"}], "created": "2005-03-08T15:28:12+01:00", "modified": "2023-11-10T11:41:25.337+01:00", "mainEntity": {"@id": "https://libris-qa.kb.se/0494flz55qzm4c5#it"}, "recordStatus": "marc:CorrectedOrRevised", "controlNumber": "18318425", "generationDate": "2021-05-12T18:36:20.87+02:00", "cataloguersNote": ["Kurs 23/24"], "marc:holdingType": "marc:SinglePartItemHolding", "generationProcess": {"@id": "https://libris.kb.se/sys/globalchanges/cleanups/2021/05/lxl-3583-remove-hold-unhandled-541-583.groovy"}, "descriptionLastModifier": {"@id": "https://libris.kb.se/library/Og"}}, {"@id": "https://libris-qa.kb.se/0494flz55qzm4c5#it", "@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "itemOf": {"@id": "https://libris-qa.kb.se/r821kzk340kvpkx#it"}, "sameAs": [{"@id": "http://libris.kb.se/resource/hold/18318425"}], "hasComponent": [{"@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "shelfMark": {"@type": "ShelfMark", "label": "Kurs REF"}, "copyNumber": "1", "availability": [{"@type": "ItemAvailability", "label": "McGee"}], "physicalLocation": ["GENERAL"]}, {"@type": "Item", "heldBy": {"@id": "https://libris.kb.se/library/Og"}, "shelfMark": {"@type": "ShelfMark", "label": "Kurs"}, "copyNumber": "2, 3, 4, 5, 6", "availability": [{"@type": "ItemAvailability", "label": ["McGee"]}], "physicalLocation": ["COURSE"]}], "inventoryLevel": "1", "marc:copyReport": {"@id": "https://id.kb.se/marc/Separate"}, "marc:completeness": {"@id": "https://id.kb.se/marc/CompletenessType-1"}, "marc:lendingPolicy": {"code": "u"}, "marc:retentionPolicy": {"@id": "https://id.kb.se/marc/PermanentlyRetained"}, "marc:acquisitionMethod": {"code": "u"}, "marc:acquisitionStatus": {"@id": "https://id.kb.se/marc/CurrentlyReceived"}, "marc:reproductionPolicy": {"code": "u"}}]}"""
        ]
        def versions = []
        for (int i = 0; i < json_versions.size()-1; ++i) { {
        }
            versions.add( mapper.readValue(json_versions[i], Map.class) )
        }

        // Common for all tests:
        def diffs = []
        for (int i = 0; i < versions.size()-1; ++i) {
            diffs.add( Diff.diff(versions[i], versions[i+1]) )
        }
        def recreatedVersions = [versions[0]]
        for (int i = 0; i < diffs.size(); ++i) {
            //System.err.println(mapper.writeValueAsString(diffs[i]))
            recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
        }
        expect:
        recreatedVersions.equals(versions)
    }

    def "linearity with nulls"() {
        given:

        def versions = [
                [
                        "a":"b"
                ],
                [
                        "b":null
                ],
                [
                        "c":"d"
                ]
        ]

        // Common for all tests:
        def diffs = []
        for (int i = 0; i < versions.size()-1; ++i) {
            diffs.add( Diff.diff(versions[i], versions[i+1]) )
        }
        def recreatedVersions = [versions[0]]
        for (int i = 0; i < diffs.size(); ++i) {
            //System.err.println(mapper.writeValueAsString(diffs[i]))
            recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
        }
        expect:
        recreatedVersions.equals(versions)
    }

    def "linearity with nulls in odd places"() {
        given:

        def versions = [
                [
                        "a":["b", "c"]
                ],
                [
                        "a":["b", null, "d", ["a": ["b", 2]]]
                ],
                [
                        "a":["b", "c", "d", ["a": ["b", 3], "c":[true, "ok", null]]]
                ],
                [
                        "a":["b", "c", "d", ["c": ["b", 3], "a":[true, "ok", "not null"]]]
                ],
        ]

        // Common for all tests:
        def diffs = []
        for (int i = 0; i < versions.size()-1; ++i) {
            diffs.add( Diff.diff(versions[i], versions[i+1]) )
        }
        def recreatedVersions = [versions[0]]
        for (int i = 0; i < diffs.size(); ++i) {
            //System.err.println(mapper.writeValueAsString(diffs[i]))
            recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
        }
        expect:
        recreatedVersions.equals(versions)
    }

    def "null preserved"() {
        given:

        def versions = [
                [
                        "a":["b", "c"]
                ],
                [
                        "a":["b", null, "d", ["a": ["b", 2]]]
                ],
                [
                        "a":["b", null, "d", ["a": ["b", 3]]]
                ],
                [
                        "a":["b", null, "d", ["a": ["b", 3]]],
                        "b": null
                ],
                [
                        "a":["b", null, "d", ["a": ["b", 4]]],
                        "b": null
                ],
        ]

        // Common for all tests:
        def diffs = []
        for (int i = 0; i < versions.size()-1; ++i) {
            diffs.add( Diff.diff(versions[i], versions[i+1]) )
        }
        def recreatedVersions = [versions[0]]
        for (int i = 0; i < diffs.size(); ++i) {
            //System.err.println(mapper.writeValueAsString(diffs[i]))
            recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
        }
        expect:
        recreatedVersions.equals(versions)
    }

}