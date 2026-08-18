package whelk.housekeeping

import spock.lang.Specification
import whelk.Document

class NotificationRulesSpec extends Specification {

    def "Barely change PrimaryContribution familyName"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Person",
                                                        "familyName": "aaa",
                                                        "givenName": "bbb",
                                                        "name": "ccc",
                                                        "lifeSpan": "2022-2023"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Person",
                                                        "familyName": "aab",
                                                        "givenName": "bbb",
                                                        "name": "ccc",
                                                        "lifeSpan": "2022-2023"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result.changed() == false
    }

    def "Change PrimaryContribution familyName"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Person",
                                                        "familyName": "aaa",
                                                        "givenName": "bbb",
                                                        "name": "ccc",
                                                        "lifeSpan": "2022-2023"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Person",
                                                        "familyName": "ddd",
                                                        "givenName": "bbb",
                                                        "name": "ccc",
                                                        "lifeSpan": "2022-2023"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Change PrimaryContribution givenName"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Person",
                                                        "familyName": "aaa",
                                                        "givenName": "bbb",
                                                        "name": "ccc",
                                                        "lifeSpan": "2022-2023"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Person",
                                                        "familyName": "aaa",
                                                        "givenName": "fff",
                                                        "name": "ccc",
                                                        "lifeSpan": "2022-2023"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Change PrimaryContribution name"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Person",
                                                        "familyName": "aaa",
                                                        "givenName": "bbb",
                                                        "name": "ccc",
                                                        "lifeSpan": "2022-2023"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Person",
                                                        "familyName": "aaa",
                                                        "givenName": "bbb",
                                                        "name": "ddd",
                                                        "lifeSpan": "2022-2023"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Add PrimaryContribution lifeSpan"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Person",
                                                        "familyName": "aaa",
                                                        "givenName": "bbb",
                                                        "name": "ccc"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Person",
                                                        "familyName": "aaa",
                                                        "givenName": "bbb",
                                                        "name": "ccc",
                                                        "lifeSpan": "2022-2023"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result.changed() == false
    }

    def "Change PrimaryContribution lifeSpan"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Person",
                                                        "familyName": "aaa",
                                                        "givenName": "bbb",
                                                        "name": "ccc",
                                                        "lifeSpan": "2022-2022"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Person",
                                                        "familyName": "aaa",
                                                        "givenName": "bbb",
                                                        "name": "ccc",
                                                        "lifeSpan": "2022-2023"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Add PrimaryContribution date of death"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Person",
                                                        "familyName": "aaa",
                                                        "givenName": "bbb",
                                                        "name": "ccc",
                                                        "lifeSpan": "2022-"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Person",
                                                        "familyName": "aaa",
                                                        "givenName": "bbb",
                                                        "name": "ccc",
                                                        "lifeSpan": "2022-2023"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result.changed() == false
    }

    def "Add PrimaryContribution date of birth"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Person",
                                                        "familyName": "aaa",
                                                        "givenName": "bbb",
                                                        "name": "ccc",
                                                        "lifeSpan": "-2023"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Person",
                                                        "familyName": "aaa",
                                                        "givenName": "bbb",
                                                        "name": "ccc",
                                                        "lifeSpan": "2022-2023"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result.changed() == false
    }

    def "Change PrimaryContribution (org) name change"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Organization",
                                                        "name": "aaa",
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Organization",
                                                        "name": "bbb",
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Change PrimaryContribution (org) isPartOf change"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Organization",
                                                        "isPartOf": ["name": "aaa"],
                                                        "marc:subordinateUnit": ["bbb"]
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Organization",
                                                        "isPartOf": ["name": "bbb"],
                                                        "marc:subordinateUnit": ["bbb"]
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Change PrimaryContribution (org) isPartOf change subordinateUnit"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Organization",
                                                        "isPartOf": ["name": "aaa"],
                                                        "marc:subordinateUnit": ["bbb"]
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Organization",
                                                        "isPartOf": ["name": "aaa"],
                                                        "marc:subordinateUnit": ["ccc"]
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Change PrimaryContribution (org) isPartOf change secondary subordinateUnit"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Organization",
                                                        "isPartOf": ["name": "aaa"],
                                                        "marc:subordinateUnit": ["bbb", "ccc"]
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Organization",
                                                        "isPartOf": ["name": "aaa"],
                                                        "marc:subordinateUnit": ["bbb", "ddd"]
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Change PrimaryContribution (meeting) place"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Meeting",
                                                        "place": "aaa"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "contribution" : [
                                        [
                                                "@type" : "PrimaryContribution",
                                                "agent" : [
                                                        "@type": "Meeting",
                                                        "place": "bbb",
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Change intended audience"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "intendedAudience" : [
                                        [
                                            "anyPropWillDo": "whatever"
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "intendedAudience" : [
                                        [
                                                "anyPropWillDo": "NOT whatever"
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.intendedAudienceChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Change agent subject"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "subject" : [
                                        [
                                                "@type": "Person",
                                                "familyName": "aaa",
                                                "givenName": "bbb",
                                                "name": "ccc",
                                                "lifeSpan": "2022-2023"
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "subject" : [
                                        [
                                                "@type": "Person",
                                                "familyName": "ddd",
                                                "givenName": "bbb",
                                                "name": "ccc",
                                                "lifeSpan": "2022-2023"
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.subjectChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Add agent subject"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "subject" : [
                                        [
                                                "@type": "Person",
                                                "name": "aaa",
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "subject" : [
                                        [
                                                "@type": "Person",
                                                "name": "aaa",
                                        ],
                                        [
                                                "@type": "Person",
                                                "name": "bbb",
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.subjectChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Remove agent subject"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "subject" : [
                                        [
                                                "@type": "Person",
                                                "name": "aaa",
                                        ],
                                        [
                                                "@type": "Person",
                                                "name": "bbb",
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "subject" : [
                                        [
                                                "@type": "Person",
                                                "name": "aaa",
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.subjectChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Flip agent subject order"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "subject" : [
                                        [
                                                "@type": "Person",
                                                "name": "aaa",
                                        ],
                                        [
                                                "@type": "Person",
                                                "name": "bbb",
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "subject" : [
                                        [
                                                "@type": "Person",
                                                "name": "bbb",
                                        ],
                                        [
                                                "@type": "Person",
                                                "name": "aaa",
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.subjectChanged(framedBefore, framedAfter)

        expect:
        result.changed() == false
    }

    def "Change main title"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "hasTitle" : [
                                ["@type": "Title", "mainTitle": "aaa"]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "hasTitle" : [
                                ["@type": "Title", "mainTitle": "bbb"]
                        ]
                ]
        ])
        var result = NotificationRules.mainTitleChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Change irrelevant part of main title"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "hasTitle" : [
                                [
                                        "@type": "Title",
                                        "mainTitle": "aaa",
                                        "marc:nonfilingChars": "4"
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "hasTitle" : [
                                [
                                        "@type": "Title",
                                        "mainTitle": "aaa",
                                        "marc:nonfilingChars": "2"
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.mainTitleChanged(framedBefore, framedAfter)

        expect:
        result.changed() == false
    }

    def "Minor title changes"() {
        expect:
        var result = NotificationRules.mainTitleChanged(
                new Document(["mainEntity": ["hasTitle": [["@type": "Title", "mainTitle": before]]]]),
                new Document(["mainEntity": ["hasTitle": [["@type": "Title", "mainTitle": after]]]])
        )
        var result2 = NotificationRules.keyTitleChanged(
                new Document(["mainEntity": ["hasTitle": [["@type": "KeyTitle", "mainTitle": before]]]]),
                new Document(["mainEntity": ["hasTitle": [["@type": "KeyTitle", "mainTitle": after]]]])
        )

        changed == result.changed()
        changed == result2.changed()

        where:
        before      | after       || changed
        'title'     | "Title"     || false
        'title '    | "title"     || false
        ' title'    | "title"     || false
        'title.'    | "title"     || false
        'title'     | "title."    || false
        'a  title'  | "a title"   || false
        'Desideria' | 'Désidéria' || false
        'titl'      | 'Title'     || false
        'titlx'     | 'Title'     || false
        'tilte'     | 'Title'     || false
        'Aker'      | 'Åker'      || false
        'Akerbar'   | 'Åkerbär'   || true
    }

    def "Change main title, dont trigger key title"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "hasTitle" : [
                                ["@type": "Title", "mainTitle": "aaa"]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "hasTitle" : [
                                ["@type": "Title", "mainTitle": "bbb"]
                        ]
                ]
        ])
        var result = NotificationRules.keyTitleChanged(framedBefore, framedAfter)

        expect:
        result.changed() == false
    }

    def "Change key title"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "hasTitle" : [
                                ["@type": "KeyTitle", "mainTitle": "aaa"]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "hasTitle" : [
                                ["@type": "KeyTitle", "mainTitle": "bbb"]
                        ]
                ]
        ])
        var result = NotificationRules.keyTitleChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Change serial relation"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "issuanceType": "Serial",
                        "continuedBy": [
                                [
                                        "@type" : "Instance",
                                        "hasTitle" : [
                                                [ "@type" : "Title", "mainTitle" : "aaa" ]
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "issuanceType": "Serial",
                        "continuedBy": [
                                [
                                        "@type" : "Instance",
                                        "hasTitle" : [
                                                [ "@type" : "Title", "mainTitle" : "bbb" ]
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.serialRelationChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Change continuedBy but not a title"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "issuanceType": "Serial",
                        "continuedBy": [
                                [
                                        "@type" : "Instance",
                                        "someOtherProperty" : "aaa",
                                        "hasTitle" : [
                                                [ "@type" : "Title", "mainTitle" : "aaa" ]
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "issuanceType": "Serial",
                        "continuedBy": [
                                [
                                        "@type" : "Instance",
                                        "someOtherProperty" : "bbb",
                                        "hasTitle" : [
                                                [ "@type" : "Title", "mainTitle" : "aaa" ]
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.serialRelationChanged(framedBefore, framedAfter)

        expect:
        result.changed() == false
    }

    def "Change serial termination"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "issuanceType": "Serial",
                        "publication" : [
                                [
                                        "@type" : "PrimaryPublication",
                                        "endYear" : "2022"
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "issuanceType": "Serial",
                        "publication" : [
                                [
                                        "@type" : "PrimaryPublication",
                                        "endYear" : "2023"
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.serialTerminationChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Add serial termination"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "issuanceType": "Serial",
                        "publication" : [
                                [
                                        "@type" : "PrimaryPublication"
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "issuanceType": "Serial",
                        "publication" : [
                                [
                                        "@type" : "PrimaryPublication",
                                        "endYear" : "2023"
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.serialTerminationChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Instance DDC change"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "classification" : [
                                [
                                        "@type" : "ClassificationDdc",
                                        "edition" : "full"
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "classification" : [
                                [
                                        "@type" : "ClassificationDdc",
                                        "edition" : "full",
                                        "someOtherProp": "whatever"
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.DDCChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Work DDC change"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "classification" : [
                                        [
                                                "@type" : "ClassificationDdc",
                                                "edition" : "full"
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "classification" : [
                                        [
                                                "@type" : "ClassificationDdc",
                                                "edition" : "full",
                                                "someOtherProp" : "whatever"
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.DDCChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Instance SAB class change"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "classification" : [
                                [
                                        "@type" : "Classification",
                                        "inScheme" : [
                                                "code" : "kssb"
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "classification" : [
                                [
                                        "@type" : "Classification",
                                        "inScheme" : [
                                                "code" : "kssb"
                                        ],
                                        "someOtherProp" : "whatever"
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.SABChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Work SAB class change"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                            "classification" : [
                                    [
                                            "@type" : "Classification",
                                            "inScheme" : [
                                                    "code" : "kssb"
                                            ]
                                    ]
                            ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "classification" : [
                                        [
                                                "@type" : "Classification",
                                                "inScheme" : [
                                                        "code" : "kssb"
                                                ],
                                                "someOtherProp" : "whatever"
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.SABChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }


    def "agentRecordChanged for Person"() {
        given:
        Document before = new Document([
                "mainEntity" : [
                        "@type"     : "Person",
                        "familyName": "aaa",
                        "givenName" : "bbb",
                        "name"      : "ccc"
                ]
        ])
        Document after = new Document([
                "mainEntity" : [
                        "@type"     : "Person",
                        "familyName": "ddd",
                        "givenName" : "bbb",
                        "name"      : "ccc"
                ]
        ])
        var result = NotificationRules.agentRecordChanged(before, after)

        expect:
        result.changed() == true
    }

    def "agentRecordChanged for Organization"() {
        given:
        Document before = new Document([
                "mainEntity" : ["@type": "Organization", "name": "aaa"]
        ])
        Document after = new Document([
                "mainEntity" : ["@type": "Organization", "name": "bbb"]
        ])
        var result = NotificationRules.agentRecordChanged(before, after)

        expect:
        result.changed() == true
    }

    def "agentRecordChanged for unchanged Person"() {
        given:
        Document before = new Document([
                "mainEntity" : ["@type": "Person", "name": "aaa"]
        ])
        Document after = new Document([
                "mainEntity" : ["@type": "Person", "name": "aaa"]
        ])
        var result = NotificationRules.agentRecordChanged(before, after)

        expect:
        result.changed() == false
    }

    def "agentRecordChanged for Jurisdiction"() {
        given:
        Document before = new Document([
                "mainEntity" : ["@type": "Jurisdiction", "name": "aaa"]
        ])
        Document after = new Document([
                "mainEntity" : ["@type": "Jurisdiction", "name": "bbb"]
        ])
        var result = NotificationRules.agentRecordChanged(before, after)

        expect:
        result.changed() == true
    }

    def "agentRecordChanged for Family"() {
        given:
        Document before = new Document([
                "mainEntity" : ["@type": "Family", "name": "aaa"]
        ])
        Document after = new Document([
                "mainEntity" : ["@type": "Family", "name": "bbb"]
        ])
        var result = NotificationRules.agentRecordChanged(before, after)

        expect:
        result.changed() == true
    }

    def "Family subject change"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "subject" : [
                                        ["@type": "Family", "name": "aaa"]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "subject" : [
                                        ["@type": "Family", "name": "bbb"]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.subjectChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Jurisdiction subject change"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "subject" : [
                                        ["@type": "Jurisdiction", "name": "aaa"]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "instanceOf" : [
                                "subject" : [
                                        ["@type": "Jurisdiction", "name": "bbb"]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.subjectChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Change serial relation via continues"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "issuanceType": "Serial",
                        "continues": [
                                [
                                        "@type" : "Instance",
                                        "hasTitle" : [
                                                [ "@type" : "Title", "mainTitle" : "aaa" ]
                                        ]
                                ]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "issuanceType": "Serial",
                        "continues": [
                                [
                                        "@type" : "Instance",
                                        "hasTitle" : [
                                                [ "@type" : "Title", "mainTitle" : "bbb" ]
                                        ]
                                ]
                        ]
                ]
        ])
        var result = NotificationRules.serialRelationChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Serial relation change ignored for non-Serial"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "issuanceType": "Monograph",
                        "continuedBy": [
                                ["@type": "Instance", "hasTitle": [["@type": "Title", "mainTitle": "aaa"]]]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "issuanceType": "Monograph",
                        "continuedBy": [
                                ["@type": "Instance", "hasTitle": [["@type": "Title", "mainTitle": "bbb"]]]
                        ]
                ]
        ])
        var result = NotificationRules.serialRelationChanged(framedBefore, framedAfter)

        expect:
        result.changed() == false
    }

    def "Remove serial termination"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "issuanceType": "Serial",
                        "publication" : [
                                ["@type": "PrimaryPublication", "endYear": "2022"]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "issuanceType": "Serial",
                        "publication" : [
                                ["@type": "PrimaryPublication"]
                        ]
                ]
        ])
        var result = NotificationRules.serialTerminationChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
        // 'before' is populated (non-null) because it had an endYear
        result.before() != null
    }

    def "Serial termination unchanged"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "issuanceType": "Serial",
                        "publication" : [
                                ["@type": "PrimaryPublication", "endYear": "2022"]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "issuanceType": "Serial",
                        "publication" : [
                                ["@type": "PrimaryPublication", "endYear": "2022"]
                        ]
                ]
        ])
        var result = NotificationRules.serialTerminationChanged(framedBefore, framedAfter)

        expect:
        result.changed() == false
    }

    def "Serial termination ignored when publication list size differs"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "issuanceType": "Serial",
                        "publication" : [
                                ["@type": "PrimaryPublication", "endYear": "2022"]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "issuanceType": "Serial",
                        "publication" : [
                                ["@type": "PrimaryPublication", "endYear": "2022"],
                                ["@type": "Publication", "endYear": "2023"]
                        ]
                ]
        ])
        var result = NotificationRules.serialTerminationChanged(framedBefore, framedAfter)

        expect:
        result.changed() == false
    }

    def "Remove DDC classification"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "classification" : [
                                ["@type": "ClassificationDdc", "edition": "full"]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "classification" : [
                                ["@type": "ClassificationDdc", "edition": "abridged"]
                        ]
                ]
        ])
        var result = NotificationRules.DDCChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

    def "Remove SAB classification"() {
        given:
        Document framedBefore = new Document([
                "mainEntity" : [
                        "classification" : [
                                ["@type": "Classification", "inScheme": ["code": "kssb"], "code": "aaa"]
                        ]
                ]
        ])
        Document framedAfter = new Document([
                "mainEntity" : [
                        "classification" : [
                                ["@type": "Classification", "inScheme": ["code": "kssb"], "code": "bbb"]
                        ]
                ]
        ])
        var result = NotificationRules.SABChanged(framedBefore, framedAfter)

        expect:
        result.changed() == true
    }

}
