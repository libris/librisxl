package whelk.housekeeping;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.util.DocumentUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static whelk.util.Jackson.mapper;

public class InquirySender extends HouseKeeper {

    private static final Logger log = LoggerFactory.getLogger(InquirySender.class);

    private static final String STATE_KEY = "CXZ inquiry email sender";
    private String status = "OK";
    private final Whelk whelk;

    public InquirySender(Whelk whelk) {
        this.whelk = whelk;
    }

    @Override
    public String getName() {
        return "Inquiry sender";
    }

    @Override
    public String getStatusDescription() {
        return status;
    }

    @Override
    public String getCronSchedule() {
        return "* * * * *";
    }

    @Override
    public void trigger() {
        Timestamp from = Timestamp.from(Instant.now().minus(1, ChronoUnit.DAYS));
        Map sendState = whelk.getStorage().getState(STATE_KEY);
        if (sendState != null && sendState.get("notifiedChangesUpTo") != null)
            from = Timestamp.from(ZonedDateTime.parse(
                    (String) sendState.get("notifiedChangesUpTo"), DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant());

        Instant notifiedChangesUpTo = from.toInstant();

        Map<String, List<Map>> heldByToUserSettings = NotificationUtils.getAllSubscribingUsers(whelk);

        Connection connection = whelk.getStorage().getOuterConnection();
        try {
            connection.setAutoCommit(false);

            String sql = "SELECT modified, data#>>'{@graph,1}' as data FROM lddb WHERE deleted = false AND data#>>'{@graph,1,@type}' IN ('InquiryAction', 'ChangeNotice') AND modified > ?;";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setTimestamp(1, from);
                statement.setFetchSize(512);
                try (ResultSet resultSet = statement.executeQuery()) {

                    while (resultSet.next()) {
                        String dataString = resultSet.getString("data");
                        Map data = mapper.readValue(dataString, Map.class);
                        NotificationUtils.NotificationType messageType =
                                NotificationUtils.NotificationType.valueOf((String) data.get("@type"));

                        /* Assume data:
                            {
                                "@id": "http://libris.kb.se.localhost:5000/xflpmzvsv9nfr5q0#it",
                                "@type": "InquiryAction",
                                "comment": [
                                    "Det här är en fråga!"
                                ],
                                "concerning": [
                                    {
                                        "@id": "http://libris.kb.se.localhost:5000/s93qhl340tcvtcp#it"
                                    }
                                ]
                            }
                         */

                        // Compile list of concerned records
                        List<String> concerningSystemIDs = new ArrayList<>();
                        for (Object link : NotificationUtils.asList(data.get("concerning"))) {
                            if (link instanceof Map<?, ?> linkMap && linkMap.get("@id") != null) {
                                String uri = (String) linkMap.get("@id");
                                String instanceId = whelk.getStorage().getSystemIdByIri(uri);
                                if (instanceId != null)
                                    concerningSystemIDs.add(instanceId);
                            }
                        }

                        // Figure out who to send to
                        Set<String> recipients = new LinkedHashSet<>();
                        String subject = NotificationUtils.subject(whelk, messageType, concerningSystemIDs);
                        for (String concerningSystemID : concerningSystemIDs) {
                            String type = whelk.getStorage().getMainEntityTypeBySystemID(concerningSystemID);
                            if (whelk.getJsonld().isSubClassOf(type, "Instance")) {
                                // Send to all holders of said instance (including Electronic)
                                List<String> libraries = whelk.getStorage().getAllLibrariesHolding(concerningSystemID);
                                recipients.addAll(getRecipientsForLibraries(libraries, heldByToUserSettings));

                                subject = NotificationUtils.subject(whelk, messageType, concerningSystemIDs, libraries);
                            } else {
                                // Send to all holders of non-electronic instances linking (in any number of steps)
                                // to whatever the message was about
                                List<String> libraries = whelk.getStorage()
                                        .followLibrariesConcernedWith(concerningSystemID, List.of("Electronic"));
                                recipients.addAll(getRecipientsForLibraries(libraries, heldByToUserSettings));
                            }
                        }

                        // Send
                        String noticeSystemId = whelk.getStorage().getSystemIdByIri((String) data.get("@id"));
                        Optional<String> creatorId = Optional.ofNullable(
                                (String) DocumentUtil.getAtPath(data, List.of("descriptionCreator", JsonLd.ID_KEY), null));
                        String body = generateEmailBody(
                                messageType,
                                noticeSystemId,
                                concerningSystemIDs,
                                NotificationUtils.asList(data.get("comment")),
                                creatorId);
                        log.info("Sending " + recipients.size() + " emails for " + noticeSystemId);
                        for (String recipient : recipients) {
                            NotificationUtils.sendEmail(recipient, subject, body);
                        }

                        Instant lastChangeObservationForInstance = resultSet.getTimestamp("modified").toInstant();
                        if (lastChangeObservationForInstance.isAfter(notifiedChangesUpTo))
                            notifiedChangesUpTo = lastChangeObservationForInstance;
                    }
                }
            }
        } catch (Throwable e) {
            status = "Failed with:\n" + e + "\nat:\n" + Arrays.toString(e.getStackTrace());
            throw new RuntimeException(e);
        } finally {
            try {
                connection.close();
            } catch (java.sql.SQLException ignored) {
            }
            if (notifiedChangesUpTo.isAfter(from.toInstant())) {
                Map newState = new HashMap();
                newState.put("notifiedChangesUpTo", notifiedChangesUpTo.atOffset(ZoneOffset.UTC).toString());
                whelk.getStorage().putState(STATE_KEY, newState);
            }
        }
    }

    private List<String> getRecipientsForLibraries(List<String> libraries, Map<String, List<Map>> heldByToUserSettings) {
        List<String> recipients = new ArrayList<>();
        for (String library : libraries) {
            List<Map> usersSubbedToLibrary = heldByToUserSettings.getOrDefault(library, List.of());
            for (Map user : usersSubbedToLibrary) {
                Object email = user.get("notificationEmail");
                if (email instanceof String emailString) {
                    recipients.add(emailString);
                }
            }
        }
        return recipients;
    }

    private String generateEmailBody(NotificationUtils.NotificationType messageType, String noticeSystemId,
                                     List<String> concerningSystemIDs, List<Object> comments,
                                     Optional<String> creatorId) {
        StringBuilder sb = new StringBuilder();

        if (comments.size() < 2) {
            for (Object comment : comments) {
                sb.append("- ").append(comment).append("\n");
            }
        } else {
            sb.append("Senaste:\n");
            sb.append("- ").append(comments.getLast()).append("\n");
            sb.append("\n");
            sb.append("Alla:\n");
            for (Object comment : comments) {
                sb.append("- ").append(comment).append("\n");
            }
        }
        sb.append("\n");

        if (messageType == NotificationUtils.NotificationType.InquiryAction) {
            sb.append("Länk till förfrågan:\n");
        } else if (messageType == NotificationUtils.NotificationType.ChangeNotice) {
            sb.append("Länk till meddelande:\n");
        }
        sb.append(NotificationUtils.makeLink(noticeSystemId)).append("\n");

        sb.append("\n");
        creatorId.ifPresent(id -> {
            String creatorLabel = "";
            Map<String, Object> definition = whelk.getJsonld().vocabIndex.get("descriptionCreator");
            if (definition != null && definition.get("labelByLang") instanceof Map<?, ?> labelByLang) {
                Object svLabel = labelByLang.get("sv");
                if (svLabel != null)
                    creatorLabel = svLabel.toString();
            }
            String creator = NotificationUtils.chipString(
                    DocumentUtil.getAtPath(whelk.loadData(id), List.of(JsonLd.GRAPH_KEY, 1), null), whelk);
            sb.append(creatorLabel).append(": ").append(creator).append("\n");
            sb.append("\n");
        });
        if (!concerningSystemIDs.isEmpty()) {
            sb.append(NotificationUtils.DIVIDER).append("\n");
            sb.append("Gäller:").append("\n");
            sb.append("\n");
            for (String systemId : concerningSystemIDs) {
                Document doc = whelk.loadEmbellished(systemId);
                sb.append(NotificationUtils.describe(doc, whelk)).append("\n");
                sb.append(NotificationUtils.makeLink(systemId)).append("\n");
                sb.append("\n");
                sb.append("\n");
            }
        }
        sb.append(NotificationUtils.EMAIL_FOOTER);

        return sb.toString();
    }
}
