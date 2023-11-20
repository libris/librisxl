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
                                                        "familyName": "aaa",
                                                        "givenName": "bbb",
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
                                                        "familyName": "aab",
                                                        "givenName": "bbb",
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
                                                        "familyName": "aaa",
                                                        "givenName": "bbb",
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
                                                        "familyName": "aaa",
                                                        "givenName": "bbc",
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
}