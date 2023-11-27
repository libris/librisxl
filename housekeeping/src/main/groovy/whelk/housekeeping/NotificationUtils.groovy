package whelk.housekeeping

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2
import org.simplejavamail.api.email.Email
import org.simplejavamail.api.mailer.Mailer
import org.simplejavamail.email.EmailBuilder
import org.simplejavamail.mailer.MailerBuilder
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.util.LegacyIntegrationTools
import whelk.util.PropertyLoader

@CompileStatic
@Log4j2
class NotificationUtils {

    public static String emailHeader = "[CXZ]"

    /**
     * Get all user settings and arrange them by requested library-uri, so that you
     * might for example start with a uri https://libris.kb.se/library/Utb1 and from it
     * get a list of user(-settings)s that subscribes to updates for things held by
     * that library
     */
    static Map<String, List<Map>> getAllSubscribingUsers(Whelk whelk) {
        Map<String, List<Map>> libraryToUserSettings = new HashMap<>()
        List<Map> allUserSettingStrings = whelk.getStorage().getAllUserData()
        for (Map settings : allUserSettingStrings) {
            if (!settings["notificationEmail"])
                continue
            settings?.requestedNotifications?.each { request ->
                if (!request instanceof Map)
                    return
                if (!request["heldBy"])
                    return

                String heldBy = request["heldBy"]
                if (!libraryToUserSettings.containsKey(heldBy))
                    libraryToUserSettings.put(heldBy, [])
                libraryToUserSettings[heldBy].add(settings)
            }
        }
        return libraryToUserSettings
    }

    static List asList(Object o) {
        if (o == null)
            return []
        if (o instanceof List)
            return o
        return [o]
    }

    static Mailer mailer = null
    static String senderAddress
    static synchronized void sendEmail(String recipient, String subject, String body) {
        if (mailer == null) {
            Properties props = PropertyLoader.loadProperties("secret")
            if (props.containsKey("smtpServer") &&
                    props.containsKey("smtpPort") &&
                    props.containsKey("smtpSender") &&
                    props.containsKey("smtpUser") &&
                    props.containsKey("smtpPassword"))
                mailer = MailerBuilder
                        .withSMTPServer(
                                (String) props.get("smtpServer"),
                                Integer.parseInt((String)props.get("smtpPort")),
                                (String) props.get("smtpUser"),
                                (String) props.get("smtpPassword")
                        ).buildMailer()
            senderAddress = props.get("smtpSender")
        }

        if (mailer) {
            Email email = EmailBuilder.startingBlank()
                    .to(recipient)
                    .withSubject(subject)
                    .from(senderAddress)
                    .withPlainText(body)
                    .buildEmail()

            log.info("Sending notification (cxz) email to " + recipient)
            mailer.sendMail(email)
        } else {
            log.info("Should now have sent notification (cxz) email to " + recipient + " but SMTP is not configured.")
            log.info(subject)
            log.info("\n" + body)
        }
    }

    static String recipientCollections(Collection<String> libraryUris) {
        libraryUris.findResults { LegacyIntegrationTools.uriToLegacySigel(it) }.unique().sort().join(' ')
    }

    // FIXME
    static String describe(Document doc, Whelk whelk) {
        JsonLd ld = whelk.jsonld
        Map data = JsonLd.frame(doc.getThingIdentifiers().first(), doc.data)
        def strings = ld.applyLensAsMapByLang(data, whelk.locales as Set, [], ['chips'])
        return strings['sv'] // ???
    }

    // FIXME
    static String makeLink(String systemId) {
        Document.BASE_URI.toString() + 'katalogisering/' + systemId // ???
    }
}
