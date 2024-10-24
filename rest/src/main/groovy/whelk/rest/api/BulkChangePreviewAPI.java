package whelk.rest.api;

import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.datatool.bulkchange.BulkChangeDocument;
import whelk.datatool.form.Transform;
import whelk.datatool.form.ModifiedThing;
import whelk.history.DocumentVersion;
import whelk.history.History;
import whelk.util.DocumentUtil;
import whelk.util.WhelkFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.RECORD_KEY;
import static whelk.util.Unicode.stripPrefix;

public class BulkChangePreviewAPI extends HttpServlet {

    private static final String BULK_CHANGE_PREVIEW_TYPE = "BulkChangePreview";
    private static final String PREVIEW_API_PATH = "/_bulk-change/preview";
    private static final int DEFAULT_LIMIT = 1;


    private Whelk whelk;

    @Override
    public void init() {
        whelk = WhelkFactory.getSingletonWhelk();
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            String id = request.getParameter("@id");
            if (id == null) {
                throw new BadRequestException("@id parameter is required");
            }
            if (!id.startsWith(Document.getBASE_URI().toString())) {
                throw new Crud.NotFoundException("Document not found");
            }
            var systemId = stripPrefix(id, Document.getBASE_URI().toString());

            int limit = nonNegativeInt(request, "_limit", DEFAULT_LIMIT);
            int offset = nonNegativeInt(request, "_offset", 0);

            var changeDoc = load(systemId);

            Map<Object, Object> result = new LinkedHashMap<>();
            result.put(JsonLd.TYPE_KEY, BULK_CHANGE_PREVIEW_TYPE);

            var transform = switch (changeDoc.getSpecification()) {
                case BulkChangeDocument.FormSpecification formSpecification ->
                        new Transform(formSpecification.matchForm(), formSpecification.targetForm(), whelk);
                case BulkChangeDocument.DeleteSpecification deleteSpecification ->
                        new Transform.MatchForm(deleteSpecification.matchForm(), whelk);
            };

            // TODO use COUNT + LIMIT & OFFSET and don't fetch all ids every time
            var sparqlPattern = transform.getSparqlPattern(whelk.getJsonld().context);
            var ids = whelk.getSparqlQueryClient().queryIdsByPattern(sparqlPattern).stream().sorted().toList();

            var itemIds = slice(ids, offset, offset + limit);
            var items = whelk.bulkLoad(itemIds)
                    .values()
                    .stream()
                    .map(doc -> makePreviewChangeSet(doc, transform))
                    .toList();

            int totalItems = ids.size();
            Offsets offsets = new Offsets(totalItems, limit, offset);
            result.putAll(makeLink(id, offset, limit));
            if (offsets.hasFirst()) {
                result.put("first", makeLink(id, offsets.first, limit));
            }
            if (offsets.hasNext()) {
                result.put("next", makeLink(id, offsets.next, limit));
            }
            if (offsets.hasPrev()) {
                result.put("prev", makeLink(id, offsets.prev, limit));
            }
            if (offsets.hasLast()) {
                result.put("last", makeLink(id, offsets.last, limit));
            }
            result.put("itemOffset", offset);
            result.put("itemsPerPage", limit);
            result.put("totalItems", totalItems);
            result.put("changeSets", transform.getChangeSets());
            result.put("items", items);

            // TODO support turtle etc?
            HttpTools.sendResponse(response, result, (String) MimeTypes.getJSONLD());
        } catch (Exception e) {
            HttpTools.sendError(response, HttpTools.mapError(e), e.getMessage(), e);
        }
    }

    private static Map<String, String> makeLink(String id, int offset, int limit) {
        var link = PREVIEW_API_PATH + "?" + JsonLd.ID_KEY + "=" + id;

        if (offset != 0) {
            link += "&_offset=" + offset;
        }

        if (limit != DEFAULT_LIMIT) {
            link += "&_limit=" + limit;
        }

        return Map.of(JsonLd.ID_KEY, link);
    }

    // FIXME mangle the data in a more ergonomic way
    @SuppressWarnings("unchecked")
    private Map<?,?> makePreviewChangeSet(Document doc, Transform transform) {
        var thing = new LinkedHashMap<String, Object>(doc.getThing());
        var record = new LinkedHashMap<String, Object>(doc.getRecord());
        // Remove @id from record to prevent from being shown as a link in the diff view
        record.remove(ID_KEY);
        thing.put(RECORD_KEY, record);
        if (transform instanceof Transform.MatchForm) {
            var result = getChangeSetsMap(List.of(doc));
            ((Map<String,Object>) DocumentUtil.getAtPath(result, List.of("changeSets", 0))).put("version", doc.getThing());
            return result;
        }
        var modified = new ModifiedThing(thing, transform, whelk.getJsonld().repeatableTerms);
        var beforeDoc = doc.clone();
        var afterDoc = doc.clone();
        ((List<Map<?,?>>) beforeDoc.data.get(JsonLd.GRAPH_KEY)).set(1, modified.getBefore());
        ((List<Map<?,?>>) afterDoc.data.get(JsonLd.GRAPH_KEY)).set(1, modified.getAfter());
        var result = getChangeSetsMap(List.of(beforeDoc, afterDoc));
        ((Map<String,Object>) DocumentUtil.getAtPath(result, List.of("changeSets", 0))).put("version", modified.getBefore());
        ((Map<String,Object>) DocumentUtil.getAtPath(result, List.of("changeSets", 1))).put("version", modified.getAfter());

        return result;
    }

    @SuppressWarnings("unchecked")
    private Map<?, ?> getChangeSetsMap(List<Document> docs) {
        var beforeDoc = docs.getFirst();
        var versions = docs.stream()
                .map(version -> new DocumentVersion(version, "", ""))
                .toList();
        History history = new History(versions, whelk.getJsonld());
        var result = history.m_changeSetsMap;
        result.put(ID_KEY, beforeDoc.getCompleteId());
        return result;
    }

    private static <T> List<T> slice(List<T> l, int from, int to) {
        int fromIx = Math.max(Math.min(from, l.size()), 0);
        int toIx = Math.min(to, l.size());
        return l.subList(fromIx, toIx);
    }

    private BulkChangeDocument load(String id) {
        Document doc = whelk.getDocument(id);
        if (doc == null) {
            throw new Crud.NotFoundException("Document not found");
        }
        try {
            return new BulkChangeDocument(doc.data);
        } catch (IllegalArgumentException e) {
            throw new BadRequestException(e.getMessage());
        }
    }

    static int nonNegativeInt(HttpServletRequest request, String param, int defaultValue) {
        try {
            if (request.getParameter(param) == null) {
                return defaultValue;
            }
            int i =  Integer.parseInt(request.getParameter(param));
            if (i < 0) {
                throw new NumberFormatException();
            }
            return i;
        } catch (NumberFormatException e) {
            throw new BadRequestException(String.format("%s must be a positive integer", param));
        }
    }

    // TODO class is duplicated in three places
    static class Offsets {
        Integer prev;
        Integer next;
        Integer first;
        Integer last;

        Offsets(int total, int limit, int offset) throws IllegalArgumentException {
            if (limit < 0) {
                throw new IllegalArgumentException("\"limit\" can't be negative.");
            }

            if (offset < 0) {
                throw new IllegalArgumentException("\"offset\" can't be negative.");
            }

            if (limit == 0) {
                return;
            }

            if (offset != 0) {
                this.first = 0;
            }

            this.prev = offset - limit;
            if (this.prev < 0) {
                this.prev = null;
            }

            this.next = offset + limit;
            if (this.next >= total) {
                this.next = null;
            } else if (offset == 0) {
                this.next = limit;
            }

            if (total % limit == 0) {
                this.last = total - limit;
            } else {
                this.last = total - (total % limit);
            }
        }

        boolean hasNext() {
            return this.next != null;
        }

        boolean hasPrev() {
            return this.prev != null;
        }

        boolean hasLast() {
            return this.last != null;
        }

        boolean hasFirst() {
            return this.first != null;
        }
    }
}
