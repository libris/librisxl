package whelk.housekeeping

import spock.lang.Specification
import whelk.Whelk
import whelk.component.PostgreSQLComponent

class NotificationUtilsSpec extends Specification {

    def "asList wraps a single object"() {
        expect:
        NotificationUtils.asList("x") == ["x"]
    }

    def "asList passes a list through"() {
        expect:
        NotificationUtils.asList(["x", "y"]) == ["x", "y"]
    }

    def "asList returns empty list for null"() {
        expect:
        NotificationUtils.asList(null) == []
    }

    def "recipientCollections maps library uris to sigels, unique and sorted"() {
        given:
        List<String> uris = [
                "https://libris.kb.se/library/Utb2",
                "https://libris.kb.se/library/Utb1",
                "https://libris.kb.se/library/Utb1", // duplicate
        ]

        expect:
        NotificationUtils.recipientCollections(uris) == "Utb1 Utb2"
    }

    def "recipientCollections drops uris that are not library uris"() {
        given:
        List<String> uris = [
                "https://libris.kb.se/library/Utb1",
                "https://example.com/not-a-library",
        ]

        expect:
        NotificationUtils.recipientCollections(uris) == "Utb1"
    }

    def "recipientCollections is empty for no libraries"() {
        expect:
        NotificationUtils.recipientCollections([]) == ""
    }

    def "getAllSubscribingUsers indexes users by held-by library uri"() {
        given:
        def storage = GroovyMock(PostgreSQLComponent)
        def whelk = GroovyMock(Whelk)
        whelk.getStorage() >> storage
        storage.getAllUserData() >> [
                [
                        "notificationEmail"      : "a@example.com",
                        "notificationCollections": [
                                ["@id": "https://libris.kb.se/library/Utb1"],
                                ["@id": "https://libris.kb.se/library/Utb2"],
                        ]
                ],
                [
                        "notificationEmail"      : "b@example.com",
                        "notificationCollections": [
                                ["@id": "https://libris.kb.se/library/Utb1"],
                        ]
                ],
        ]

        when:
        Map<String, List<Map>> result = NotificationUtils.getAllSubscribingUsers(whelk)

        then:
        result.keySet() == ["https://libris.kb.se/library/Utb1", "https://libris.kb.se/library/Utb2"] as Set
        result["https://libris.kb.se/library/Utb1"]*.notificationEmail == ["a@example.com", "b@example.com"]
        result["https://libris.kb.se/library/Utb2"]*.notificationEmail == ["a@example.com"]
    }

    def "getAllSubscribingUsers skips users without a notification email"() {
        given:
        def storage = GroovyMock(PostgreSQLComponent)
        def whelk = GroovyMock(Whelk)
        whelk.getStorage() >> storage
        storage.getAllUserData() >> [
                [
                        // no notificationEmail
                        "notificationCollections": [
                                ["@id": "https://libris.kb.se/library/Utb1"],
                        ]
                ],
        ]

        when:
        Map<String, List<Map>> result = NotificationUtils.getAllSubscribingUsers(whelk)

        then:
        result.isEmpty()
    }

    def "getAllSubscribingUsers skips collection entries without an @id"() {
        given:
        def storage = GroovyMock(PostgreSQLComponent)
        def whelk = GroovyMock(Whelk)
        whelk.getStorage() >> storage
        storage.getAllUserData() >> [
                [
                        "notificationEmail"      : "a@example.com",
                        "notificationCollections": [
                                ["noId": "whatever"],
                                ["@id": "https://libris.kb.se/library/Utb1"],
                        ]
                ],
        ]

        when:
        Map<String, List<Map>> result = NotificationUtils.getAllSubscribingUsers(whelk)

        then:
        result.keySet() == ["https://libris.kb.se/library/Utb1"] as Set
    }
}
