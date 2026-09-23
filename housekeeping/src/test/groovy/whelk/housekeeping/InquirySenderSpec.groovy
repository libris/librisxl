package whelk.housekeeping

import spock.lang.Specification
import whelk.Whelk

class InquirySenderSpec extends Specification {

    private String bodyFor(List<String> comments) {
        def sender = new InquirySender((Whelk) null)
        // concerningSystemIDs empty and creatorId absent -> no Whelk/DB access,
        // exercising only the comment-formatting branch.
        return sender.generateEmailBody(
                NotificationUtils.NotificationType.InquiryAction,
                "somesystemid",
                [],
                comments,
                Optional.empty())
    }

    def "single comment is rendered as a plain bullet without Senaste/Alla headers"() {
        when:
        String body = bodyFor(["Det här är en fråga!"])

        then:
        body.contains("- Det här är en fråga!")
        !body.contains("Senaste:")
        !body.contains("Alla:")
    }

    def "no comments renders neither bullet nor Senaste/Alla headers"() {
        when:
        String body = bodyFor([])

        then:
        !body.contains("Senaste:")
        !body.contains("Alla:")
    }

    def "two or more comments render Senaste (latest) and Alla (all) sections"() {
        when:
        String body = bodyFor(["first", "second", "third"])

        then:
        body.contains("Senaste:")
        body.contains("Alla:")
        // 'Senaste' shows the last comment
        body.indexOf("Senaste:") < body.indexOf("- third")
        // 'Alla' lists every comment
        body.contains("- first")
        body.contains("- second")
        body.contains("- third")
    }

    def "InquiryAction produces the inquiry link label"() {
        when:
        String body = bodyFor(["q"])

        then:
        body.contains("Länk till förfrågan:")
    }

    def "ChangeNotice produces the notice link label"() {
        given:
        def sender = new InquirySender((Whelk) null)

        when:
        String body = sender.generateEmailBody(
                NotificationUtils.NotificationType.ChangeNotice,
                "somesystemid",
                [],
                ["q"],
                Optional.empty())

        then:
        body.contains("Länk till meddelande:")
    }
}
