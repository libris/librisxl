package whelk.housekeeping;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.simplejavamail.api.email.Email;
import org.simplejavamail.api.mailer.Mailer;
import org.simplejavamail.email.EmailBuilder;
import org.simplejavamail.mailer.MailerBuilder;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.util.LegacyIntegrationTools;
import whelk.util.PropertyLoader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

public class NotificationUtils {

    private static final Logger log = LoggerFactory.getLogger(NotificationUtils.class);

    private static final String EMAIL_HEADER = "[CXZ]";
    static final String DIVIDER = "-------------------------------------------";
    static final String EMAIL_FOOTER = "\n" + DIVIDER + """

            Läs mer om CXZ-meddelanden:
            https://metadatabyran.kb.se/arbetsfloden/hantera-poster-i-libris/cxz---andringsmeddelanden-och-fragor-i-libris
            """;

    enum NotificationType {
        ChangeObservation,
        ChangeNotice,
        InquiryAction
    }

    static String subject(Whelk whelk, NotificationType notificationType, List<String> concerningSystemIDs) {
        return subject(whelk, notificationType, concerningSystemIDs, List.of());
    }

    static String subject(Whelk whelk, NotificationType notificationType, List<String> concerningSystemIDs,
                          List<String> libraryUris) {
        String typeLabel = "";
        Map<String, Object> typeDefinition = whelk.getJsonld().vocabIndex.get(notificationType.toString());
        if (typeDefinition != null && typeDefinition.get("labelByLang") instanceof Map<?, ?> labelByLang) {
            Object svLabel = labelByLang.get("sv");
            if (svLabel != null)
                typeLabel = svLabel.toString();
        }

        List<String> concernedLibrisIDs = new ArrayList<>();
        if (concerningSystemIDs != null) {
            for (String systemId : concerningSystemIDs) {
                Document doc = whelk.loadEmbellished(systemId);
                if (whelk.getJsonld().isSubClassOf(doc.getThingType(), "Instance")) {
                    concernedLibrisIDs.add(doc.getControlNumber());
                }
            }
        }
        String librisIDs = String.join(", ", concernedLibrisIDs);
        if (!librisIDs.isEmpty()) {
            librisIDs = " Libris-ID " + librisIDs;
        }

        String collections = recipientCollections(libraryUris);
        return EMAIL_HEADER + " " + typeLabel + "." + librisIDs + (collections.isEmpty() ? "" : " " + collections);
    }

    /**
     * Get all user settings and arrange them by requested library-uri, so that you
     * might for example start with a uri https://libris.kb.se/library/Utb1 and from it
     * get a list of user(-settings)s that subscribes to updates for things held by
     * that library
     */
    static Map<String, List<Map>> getAllSubscribingUsers(Whelk whelk) {
        Map<String, List<Map>> libraryToUserSettings = new HashMap<>();
        List<Map> allUserSettings = whelk.getStorage().getAllUserData();
        for (Map settings : allUserSettings) {
            if (!isTruthy(settings.get("notificationEmail")))
                continue;
            for (Object request : asList(settings.get("notificationCollections"))) {
                if (!(request instanceof Map<?, ?> requestMap))
                    continue;
                Object heldBy = requestMap.get("@id");
                if (heldBy == null)
                    continue;

                libraryToUserSettings
                        .computeIfAbsent((String) heldBy, k -> new ArrayList<>())
                        .add(settings);
            }
        }
        return libraryToUserSettings;
    }

    static List<Object> asList(Object o) {
        if (o == null)
            return new ArrayList<>();
        if (o instanceof List<?> list)
            return new ArrayList<>(list);
        List<Object> single = new ArrayList<>();
        single.add(o);
        return single;
    }

    private static Mailer mailer = null;
    private static String senderAddress;

    static synchronized void sendEmail(String recipient, String subject, String body) {
        if (mailer == null) {
            Properties props = PropertyLoader.loadProperties("secret");
            if (props.containsKey("smtpServer") &&
                    props.containsKey("smtpPort") &&
                    props.containsKey("smtpSender") &&
                    props.containsKey("smtpUser") &&
                    props.containsKey("smtpPassword"))
                mailer = MailerBuilder
                        .withSMTPServer(
                                (String) props.get("smtpServer"),
                                Integer.parseInt((String) props.get("smtpPort")),
                                (String) props.get("smtpUser"),
                                (String) props.get("smtpPassword")
                        ).buildMailer();
            senderAddress = (String) props.get("smtpSender");
        }

        if (mailer != null) {
            // unclear if simplejavamail checks subject length
            subject = subject.substring(0, Math.min(subject.length(), 800));

            Email email = EmailBuilder.startingBlank()
                    .to(recipient)
                    .withSubject(subject)
                    .from(senderAddress)
                    .withPlainText(body)
                    .buildEmail();

            log.info("Sending notification (cxz) email to " + recipient);
            mailer.sendMail(email);
        } else {
            log.info("Should now have sent notification (cxz) email to " + recipient + " but SMTP is not configured.");
            log.info(subject);
            log.info("\n" + body);
        }
    }

    static String recipientCollections(Collection<String> libraryUris) {
        Set<String> sigels = new TreeSet<>();
        for (String libraryUri : libraryUris) {
            String sigel = LegacyIntegrationTools.uriToLegacySigel(libraryUri);
            if (sigel != null)
                sigels.add(sigel);
        }
        return String.join(" ", sigels);
    }

    // TODO use fresnel:Format mechanism here when stable
    static String describe(Document doc, Whelk whelk) {
        Map data = JsonLd.frame(doc.getThingIdentifiers().getFirst(), doc.data);
        StringBuilder s = new StringBuilder();
        s.append(chipString(data, whelk));

        for (String p : List.of("responsibilityStatement", "publication", "extent")) {
            for (Object value : asList(data.get(p))) {
                s.append("\n").append(chipString(value, whelk));
            }
        }
        s.append("\n").append("Kontrollnummer: ").append(doc.getControlNumber());
        for (String isbn : doc.getIsbnValues()) {
            s.append("\n").append("ISBN: ").append(isbn);
        }

        return s.toString();
    }

    // TODO use fresnel:Format mechanism here when stable
    static String chipString(Object data, Whelk whelk) {
        if (!(data instanceof Map<?, ?> map)) {
            return String.valueOf(data);
        }

        try {
            Map chip = whelk.getJsonld().applyLensAsMapByLang(
                    (Map) map, new LinkedHashSet<>(whelk.getLocales()), List.of(), List.of("chips"));
            return (String) chip.get("sv");
        } catch (Exception ignored) {
            return "";
        }
    }

    // FIXME
    static String makeLink(String systemId) {
        return Document.getBASE_URI().toString() + "katalogisering/" + systemId; // ???
    }

    private static boolean isTruthy(Object o) {
        if (o == null)
            return false;
        if (o instanceof String s)
            return !s.isEmpty();
        return true;
    }
}
