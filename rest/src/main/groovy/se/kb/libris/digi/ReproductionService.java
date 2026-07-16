package se.kb.libris.digi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static se.kb.libris.digi.RequestException.badRequest;
import static se.kb.libris.digi.Util.asList;
import static se.kb.libris.digi.Util.asMap;
import static se.kb.libris.digi.Util.asSet;
import static se.kb.libris.digi.Util.getAtPath;
import static se.kb.libris.digi.Util.isTruthy;

class ReproductionService {
    private static final Map<String, Object> DIGI = Map.of("@id", "https://libris.kb.se/library/DIGI");
    private static final Map<String, Object> DST = Map.of("@id", "https://libris.kb.se/library/DST");
    private static final Map<String, Object> ONLINE = Map.of("@id", "https://id.kb.se/term/rda/OnlineResource");
    private static final Map<String, Object> FREELY_AVAILABLE = Map.of("@id", "https://id.kb.se/policy/freely-available");

    private static final Pattern YEAR = Pattern.compile("\\d\\d\\d\\d.*");

    private final XL xl;

    ReproductionService(XL xl) {
        this.xl = xl;
    }

    String createDigitalReproduction(Map<String, Object> electronicThing, boolean extractWork) throws IOException {
        Map<String, Object> reproductionOf = asMap(electronicThing.get("reproductionOf"));
        String requestedId = (String) reproductionOf.get("@id");

        Map<String, Object> physicalThing = xl.get(requestedId)
                .map(doc -> mainEntity(doc.data()))
                .orElseThrow(() -> badRequest("Thing linked in reproductionOf does not exist: " + requestedId));

        if ("Electronic".equals(physicalThing.get("@type"))) {
            throw badRequest("Thing linked in reproductionOf cannot be Electronic");
        }
        if ("DigitalResource".equals(physicalThing.get("@type"))) {
            throw badRequest("Thing linked in reproductionOf cannot be DigitalResource");
        }

        List<Object> holdingsToCreate = asList(getAtPath(electronicThing, List.of("@reverse", "itemOf"), List.of()));

        List<Object> badHeldBy = new ArrayList<>();
        for (Object item : holdingsToCreate) {
            Object heldBy = getAtPath(item, List.of("heldBy", "@id"), "MISSING");
            if (xl.get(String.valueOf(heldBy)).isEmpty()) {
                badHeldBy.add(heldBy);
            }
        }
        if (!badHeldBy.isEmpty()) {
            throw badRequest("No such library: " + badHeldBy);
        }

        reproductionOf.put("@id", physicalThing.get("@id")); // if link was to sameAs

        electronicThing.put("instanceOf", extractWork
                ? new LinkedHashMap<>(Map.of("@id", xl.ensureExtractedWork((String) physicalThing.get("@id"))))
                : physicalThing.get("instanceOf"));

        if (isTruthy(physicalThing.get("hasTitle"))) {
            electronicThing.put("hasTitle", physicalThing.get("hasTitle"));
        }

        if (isTruthy(physicalThing.get("issuanceType")) && !isTruthy(electronicThing.get("issuanceType"))) {
            electronicThing.put("issuanceType", physicalThing.get("issuanceType"));
        }

        if (isOnline(electronicThing)) {
            String key = "DigitalResource".equals(electronicThing.get("@type")) ? "category" : "carrierType";
            Set<Object> values = asSet(electronicThing.get(key));
            values.add(ONLINE);
            electronicThing.put(key, values);
        }

        Map<String, Object> record = asMap(electronicThing.remove("meta"));

        Set<Object> bibliography = asSet(record.get("bibliography"));
        if (isDigitaliseratSvensktTryck(physicalThing, electronicThing)) {
            bibliography.add(DST);
        }
        bibliography.add(DIGI);
        record.put("bibliography", bibliography);

        String electronicId = xl.create(record, electronicThing);
        for (Object item : holdingsToCreate) {
            createHoldingFor(electronicId, asMap(item));
        }

        return electronicId;
    }

    boolean isDigitaliseratSvensktTryck(Map<String, Object> physicalThing, Map<String, Object> electronicThing) {
        if (!isFreelyAvailable(electronicThing)) {
            return false;
        }
        return asList(physicalThing.get("publication")).stream()
                .map(Util::asMap)
                .anyMatch(p -> publishedIn(p, "sw") || (publishedIn(p, "fi") && pre1810(p)));
    }

    static boolean isFreelyAvailable(Map<String, Object> thing) {
        List<Object> usageAndAccess = asList(thing.get("usageAndAccessPolicy"));
        usageAndAccess.addAll(asList(getAtPath(thing, List.of("associatedMedia", "*", "usageAndAccessPolicy"), List.of())));
        return usageAndAccess.stream().anyMatch(FREELY_AVAILABLE::equals);
    }

    static boolean publishedIn(Map<String, Object> publication, String countryCode) {
        Object country = publication.get("country");
        return isTruthy(country)
                && country instanceof Map
                && ("https://id.kb.se/country/" + countryCode).equals(((Map<?, ?>) country).get("@id"));
    }

    boolean pre1810(Map<String, Object> publication) {
        return parseYear(str(publication.get("year"))) <= 1809
                || parseYear(str(publication.get("date"))) <= 1809;
    }

    static int parseYear(String date) {
        return (date != null && YEAR.matcher(date).matches())
                ? Integer.parseInt(date.substring(0, 4))
                : Integer.MAX_VALUE;
    }

    static boolean isOnline(Map<String, Object> thing) {
        return isTruthy(thing.get("hasRepresentation"))
                || !asList(getAtPath(thing, List.of("associatedMedia", "*", "uri"), List.of())).isEmpty();
    }

    void createHoldingFor(String thingId, Map<String, Object> item) throws IOException {
        Map<String, Object> record = asMap(item.remove("meta"));
        Object heldById = getAtPath(item, List.of("heldBy", "@id"));

        List<Object> hasComponent = asList(item.get("hasComponent"));
        if (hasComponent.isEmpty()) {
            hasComponent = new ArrayList<>(List.of(new LinkedHashMap<String, Object>()));
        }

        List<Object> components = new ArrayList<>();
        for (Object c : hasComponent) {
            Map<String, Object> component = new LinkedHashMap<>(asMap(c));
            component.put("@type", "Item");
            component.put("heldBy", heldByLink(heldById));
            components.add(component);
        }

        Map<String, Object> holding = new LinkedHashMap<>(item);
        holding.put("@type", "Item");
        holding.put("itemOf", new LinkedHashMap<>(Map.of("@id", thingId)));
        holding.put("heldBy", heldByLink(heldById));
        holding.put("hasComponent", components);

        xl.create(record, holding);
    }

    private static Map<String, Object> heldByLink(Object heldById) {
        Map<String, Object> link = new LinkedHashMap<>();
        link.put("@id", heldById);
        return link;
    }

    private static String str(Object o) {
        return o != null ? String.valueOf(o) : null;
    }

    /** The mainEntity, i.e. the second node of the record's @graph. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> mainEntity(Map<String, Object> data) {
        return (Map<String, Object>) ((List<Object>) data.get("@graph")).get(1);
    }
}
