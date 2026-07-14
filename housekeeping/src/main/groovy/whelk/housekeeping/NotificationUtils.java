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

    private static final String emailHeader = "[CXZ]"
    static final String DIVIDER = '-------------------------------------------'
    static final String EMAIL_FOOTER = """
        $DIVIDER
        Läs mer om CXZ-meddelanden:
        https://metadatabyran.kb.se/arbetsfloden/hantera-poster-i-libris/cxz---andringsmeddelanden-och-fragor-i-libris
        """.stripIndent()

    enum NotificationType {
        ChangeObservation,
        ChangeNotice,
        InquiryAction
    }

    static String subject(Whelk whelk, NotificationType notificationType, List<String> concerningSystemIDs, List<String> libraryUris = []) {
        var typeLabel = whelk.jsonld.vocabIndex[notificationType.toString()]?['labelByLang']?['sv'] ?: ""

        List<String> concernedLibrisIDs = []
        if (concerningSystemIDs) {
            for (String systemId : concerningSystemIDs) {
                Document doc = whelk.loadEmbellished(systemId)
                if ( whelk.getJsonld().isSubClassOf(doc.getThingType(), "Instance") ) {
                    concernedLibrisIDs.add( doc.getControlNumber() )
                }
            }
        }
        String librisIDs = String.join(", ", concernedLibrisIDs)
        if (librisIDs.length() > 0) {
            librisIDs = " Libris-ID " + librisIDs
        }

        String collections = recipientCollections(libraryUris)
        return "$emailHeader ${typeLabel}.${librisIDs}${collections ? ' ' : ''}${collections}"
    }

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
            settings?.notificationCollections?.each { request ->
                if (!request instanceof Map)
                    return
                if (!request["@id"])
                    return

                String heldBy = request["@id"]
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
            // unclear if simplejavamail checks subject length
            subject = subject.substring(0, Math.min(subject.length(), 800))

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

    // TODO use fresnel:Format mechanism here when stable
    static String describe(Document doc, Whelk whelk) {
        Map data = JsonLd.frame(doc.getThingIdentifiers().first(), doc.data)
        StringBuilder s = new StringBuilder()
        s.append(chipString(data, whelk))

        ['responsibilityStatement', 'publication', 'extent'].each {p ->
            asList(data[p]).each { s.append("\n").append(chipString(it, whelk)) }
        }
        s.append("\n").append("Kontrollnummer: ").append(doc.getControlNumber())
        List<String> isbnValues = doc.getIsbnValues()
        if (isbnValues) {
            for (String isbn : isbnValues) {
                s.append("\n").append("ISBN: ").append(isbn)
            }
        }

        return s.toString()
    }

    // TODO use fresnel:Format mechanism here when stable
    static String chipString(Object data, Whelk whelk) {
        if (data !instanceof Map) {
            return data
        }

        try {
            return whelk.jsonld.applyLensAsMapByLang(data, whelk.locales as Set, [], ['chips'])['sv']
        } catch (Exception ignored) {
            return ""
        }
    }

    // FIXME
    static String makeLink(String systemId) {
        Document.BASE_URI.toString() + 'katalogisering/' + systemId // ???
    }
}
