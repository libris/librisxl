package whelk.housekeeping

import spock.lang.Specification
import whelk.Whelk

import static whelk.util.Jackson.mapper

class NotificationSenderSpec extends Specification {

    private static String observation(String categoryId) {
        return mapper.writeValueAsString([
                "@graph": [
                        ["@id": "https://libris.kb.se/record", "@type": "Record"],
                        [
                                "@id"     : "https://libris.kb.se/record#it",
                                "@type"   : "ChangeObservation",
                                "category": ["@id": categoryId],
                        ]
                ]
        ])
    }

    def "matches returns the observation whose category equals the trigger"() {
        given:
        def sender = new NotificationSender((Whelk) null)
        List observations = [
                observation("https://id.kb.se/changecategory/maintitle"),
                observation("https://id.kb.se/changecategory/keytitle"),
        ]

        when:
        Map result = sender.matches("https://id.kb.se/changecategory/keytitle", observations)

        then:
        result["@graph"][1]["category"]["@id"] == "https://id.kb.se/changecategory/keytitle"
    }

    def "matches returns null when no category matches the trigger"() {
        given:
        def sender = new NotificationSender((Whelk) null)
        List observations = [
                observation("https://id.kb.se/changecategory/maintitle"),
        ]

        expect:
        sender.matches("https://id.kb.se/changecategory/sabclassification", observations) == null
    }

    def "matches returns null for an empty observation list"() {
        given:
        def sender = new NotificationSender((Whelk) null)

        expect:
        sender.matches("https://id.kb.se/changecategory/maintitle", []) == null
    }
}
