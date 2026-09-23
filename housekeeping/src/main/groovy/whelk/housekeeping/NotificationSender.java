package whelk.housekeeping;

import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;

import java.sql.Array;
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
import java.util.Set;

import static whelk.util.Jackson.mapper;

public class NotificationSender extends HouseKeeper {

    private static final String STATE_KEY = "CXZ notification email sender";
    private String status = "OK";
    private final Whelk whelk;

    public NotificationSender(Whelk whelk) {
        this.whelk = whelk;
    }

    @Override
    public String getName() {
        return "Notifications sender";
    }

    @Override
    public String getStatusDescription() {
        return status;
    }

    @Override
    public String getCronSchedule() {
        return "0 6 * * *";
    }

    @Override
    public void trigger() {
        Map<String, List<Map>> heldByToUserSettings = NotificationUtils.getAllSubscribingUsers(whelk);

        // Determine the time interval of ChangeObservations to consider
        Timestamp from = Timestamp.from(Instant.now().minus(1, ChronoUnit.DAYS)); // Default to last 24h if first time.
        Map sendState = whelk.getStorage().getState(STATE_KEY);
        if (sendState != null && sendState.get("notifiedChangesUpTo") != null)
            from = Timestamp.from(ZonedDateTime.parse(
                    (String) sendState.get("notifiedChangesUpTo"), DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant());

        Instant notifiedChangesUpTo = from.toInstant();

        Connection connection = whelk.getStorage().getOuterConnection();
        try {
            connection.setAutoCommit(false);

            String sql = "SELECT MAX(created) as lastChange, data#>>'{@graph,1,concerning,@id}' as concerningUri, ARRAY_AGG(data::text) as data FROM lddb WHERE data#>>'{@graph,1,@type}' = 'ChangeObservation' AND created > ? GROUP BY data#>>'{@graph,1,concerning,@id}';";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setTimestamp(1, from);
                statement.setFetchSize(512);
                try (ResultSet resultSet = statement.executeQuery()) {

                    while (resultSet.next()) {
                        String concerningUri = resultSet.getString("concerningUri");
                        Array changeObservationsArray = resultSet.getArray("data");
                        List<Object> changeObservationsForConcerned = new ArrayList<>();
                        for (Object o : (Object[]) changeObservationsArray.getArray()) {
                            changeObservationsForConcerned.add(o);
                        }

                        sendFor(concerningUri, heldByToUserSettings, changeObservationsForConcerned);

                        Instant lastChangeObservationForInstance = resultSet.getTimestamp("lastChange").toInstant();
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
                // Matching the Groovy original, which did not handle close() failures here.
            }
            if (notifiedChangesUpTo.isAfter(from.toInstant())) {
                Map newState = new HashMap();
                newState.put("notifiedChangesUpTo", notifiedChangesUpTo.atOffset(ZoneOffset.UTC).toString());
                whelk.getStorage().putState(STATE_KEY, newState);
            }
        }
    }

    private void sendFor(String concerningUri, Map<String, List<Map>> heldByToUserSettings,
                         List<Object> changeObservationsForConcerned) {
        String concerningSystemId = whelk.getStorage().getSystemIdByIri(concerningUri);
        String type = whelk.getStorage().getMainEntityTypeBySystemID(concerningSystemId);

        if (whelk.getJsonld().isSubClassOf(type, "Instance"))
            sendForInstance(concerningSystemId, heldByToUserSettings, changeObservationsForConcerned);
        else if (whelk.getJsonld().isSubClassOf(type, "Agent"))
            sendForAgent(concerningSystemId, heldByToUserSettings, changeObservationsForConcerned);
    }

    private void sendForAgent(String concerningId, Map<String, List<Map>> heldByToUserSettings,
                              List<Object> changeObservationsForConcerned) throws RuntimeException {
        List<String> concernedLibraries = whelk.getStorage().followLibrariesConcernedWith(concerningId, List.of("Electronic"));
        String subject = NotificationUtils.subject(whelk, NotificationUtils.NotificationType.ChangeObservation,
                List.of(concerningId), concernedLibraries);

        List<Map> changeObservationMaps = new ArrayList<>();
        for (Object observationDataString : changeObservationsForConcerned) {
            Map changeObservationMap = readMap((String) observationDataString);
            if (changeObservationMap != null)
                changeObservationMaps.add(changeObservationMap);
        }

        Set<Object> alreadySentTo = new LinkedHashSet<>();

        for (String library : concernedLibraries) {
            List<Map> users = heldByToUserSettings.get(library);
            if (users == null)
                continue;
            for (Map user : users) {
                if (!subscribesToAgentChanges(user))
                    continue;

                Object email = user.get("notificationEmail");
                if (!changeObservationMaps.isEmpty() && email instanceof String emailString
                        && !emailString.isEmpty() && !alreadySentTo.contains(email)) {
                    String body = generateEmailBody(concerningId, new LinkedHashSet<>(changeObservationMaps));
                    NotificationUtils.sendEmail(emailString, subject, body);
                    alreadySentTo.add(email);
                }
            }
        }
    }

    private boolean subscribesToAgentChanges(Map user) {
        for (Object category : NotificationUtils.asList(user.get("notificationCategories"))) {
            if (category instanceof Map<?, ?> categoryMap
                    && "https://id.kb.se/changecategory/agent".equals(categoryMap.get("@id")))
                return true;
        }
        return false;
    }

    private void sendForInstance(String instanceId, Map<String, List<Map>> heldByToUserSettings,
                                 List<Object> changeObservationsForInstance) {
        List<String> libraries = whelk.getStorage().getAllLibrariesHolding(instanceId);
        String subject = NotificationUtils.subject(whelk, NotificationUtils.NotificationType.ChangeObservation,
                List.of(instanceId), libraries);

        for (String library : libraries) {
            List<Map> users = heldByToUserSettings.get(library);
            if (users == null)
                continue;
            for (Map user : users) {
                /* 'user' is now a map looking something like this:
                {
                    "notificationEmail": "...",
                    "notificationCategories": [
                        {
                            "@id": "https://id.kb.se/changecategory/maintitle"
                        },
                        ...
                    ],
                    "notificationCollections": [
                        {
                            "@id": "https://libris.kb.se/library/Utb1"
                        },
                        ...
                    ]
                }
                */

                Set<Map> matchedObservations = new LinkedHashSet<>();

                for (Object category : NotificationUtils.asList(user.get("notificationCategories"))) {
                    if (!(category instanceof Map<?, ?> request))
                        continue;
                    String trigger = (String) request.get("@id");
                    if (trigger != null) {
                        Map triggeredObservation = matches(trigger, changeObservationsForInstance);
                        if (triggeredObservation != null) {
                            matchedObservations.add(triggeredObservation);
                        }
                    }
                }

                Object email = user.get("notificationEmail");
                if (!matchedObservations.isEmpty() && email instanceof String emailString && !emailString.isEmpty()) {
                    String body = generateEmailBody(instanceId, matchedObservations);
                    NotificationUtils.sendEmail(emailString, subject, body);
                }
            }
        }
    }

    private Map matches(String trigger, List<Object> changeObservationsForInstance) {
        for (Object obj : changeObservationsForInstance) {
            Map changeObservationMap = readMap((String) obj);
            if (changeObservationMap == null)
                continue;
            Object category = Document._get(List.of("@graph", 1, "category", "@id"), changeObservationMap);
            if (category != null && category.equals(trigger))
                return changeObservationMap;
        }
        return null;
    }

    private static Map readMap(String json) {
        try {
            return mapper.readValue(json, Map.class);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String generateEmailBody(String changedInstanceId, Set<Map> triggeredObservations) {
        Document current = whelk.getStorage().load(changedInstanceId);
        StringBuilder sb = new StringBuilder();
        sb.append("** Automatiskt ändringsmeddelande **\n");
        sb.append("\n");
        boolean commentsRendered = false;
        for (Map observation : triggeredObservations) {
            Object observationUriValue = Document._get(List.of("@graph", 1, "@id"), observation);
            if (!(observationUriValue instanceof String observationUri) || observationUri.isEmpty())
                continue;

            if (!commentsRendered) {
                commentsRendered = true;
                Object comments = Document._get(List.of("@graph", 1, "comment"), observation);

                if (comments instanceof List<?> commentList) {
                    sb.append("\nÄndringsanmärkningar:\n");
                    for (Object comment : commentList)
                        sb.append("\t- ").append(String.valueOf(comment).replace("\n", "\n\t")).append("\n");
                }
                sb.append("\n");
            }

            String observationId = whelk.getStorage().getSystemIdByIri(observationUri);
            Document embellishedObservation = whelk.loadEmbellished(observationId);
            Map framed = JsonLd.frame(observationUri, embellishedObservation.data);

            Map category = whelk.getJsonld().applyLensAsMapByLang(
                    (Map) framed.get("category"), Set.of("sv"), List.of(), List.of("chips"));
            sb.append(category.get("sv"));

            appendRepresentation(sb, framed.get("representationAfter"), "\n  Nu:", "\n  Nu: ");
            appendRepresentation(sb, framed.get("representationBefore"), "\n  Tidigare: ", "\n  Tidigare: ");

            sb.append("\n\n");
        }

        sb.append("\n");
        sb.append(NotificationUtils.DIVIDER).append("\n");
        sb.append("Gäller:").append("\n");
        sb.append("\n");
        whelk.embellish(current);
        sb.append(NotificationUtils.describe(current, whelk)).append("\n");
        sb.append(NotificationUtils.makeLink(changedInstanceId)).append("\n");
        sb.append(NotificationUtils.EMAIL_FOOTER);

        return sb.toString();
    }

    private void appendRepresentation(StringBuilder sb, Object representation, String singularPrefix, String listPrefix) {
        if (representation instanceof Map<?, ?> map) {
            Map chip = whelk.getJsonld().applyLensAsMapByLang((Map) map, Set.of("sv"), List.of(), List.of("chips"));
            sb.append(singularPrefix).append(chip.get("sv"));
        } else if (representation instanceof List<?> list) {
            sb.append(listPrefix);
            for (Object item : list) {
                Map chip = whelk.getJsonld().applyLensAsMapByLang((Map) item, Set.of("sv"), List.of(), List.of("chips"));
                sb.append((String) chip.get("sv")).append(", ");
            }
        }
    }
}
