package whelk.housekeeping

import spock.lang.Specification
import whelk.Document

class NotificationRulesSpec extends Specification {

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
        Tuple result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result[0] == true
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
                                                        "givenName": "bbc",
                                                        "name": "ccc",
                                                        "lifeSpan": "2022-2023"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        Tuple result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result[0] == true
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
                                                        "name": "ccd",
                                                        "lifeSpan": "2022-2023"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ])
        Tuple result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result[0] == true
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
        Tuple result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result[0] == false
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
        Tuple result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result[0] == true
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
        Tuple result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result[0] == true
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
        Tuple result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result[0] == true
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
        Tuple result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result[0] == true
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
        Tuple result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result[0] == true
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
        Tuple result = NotificationRules.primaryContributionChanged(framedBefore, framedAfter)

        expect:
        result[0] == true
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
        Tuple result = NotificationRules.intendedAudienceChanged(framedBefore, framedAfter)

        expect:
        result[0] == true
    }

}