package whelk.rest.api;

import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.datatool.bulkchange.BulkChangeDocument;
import whelk.datatool.form.FormDiff;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static whelk.util.Unicode.stripPrefix;

public class BulkChangePreviewAPI extends HttpServlet {

    private static final String BULK_CHANGE_PREVIEW_TYPE = "BulkChangePreview";

    private Whelk whelk;

    @Override
    public void init() {
        whelk = WhelkFactory.getSingletonWhelk();
    }

    // TODO generate @id / first / last / next / prev links
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
            id = stripPrefix(id, Document.getBASE_URI().toString());

            int limit = parsePositiveInt(request, "_limit", 5);
            int offset = parsePositiveInt(request, "_offset", 0);

            var changeDoc = load(id);

            Map<Object, Object> result = new HashMap<>();
            result.put(JsonLd.TYPE_KEY, BULK_CHANGE_PREVIEW_TYPE);

            switch (changeDoc.getSpecification()) {
                case BulkChangeDocument.FormSpecification formSpecification -> {
                    var diff = new FormDiff(formSpecification.matchForm(), formSpecification.targetForm());
                    var match = diff.getMatchFormCopyWithoutMarkerIds();
                    var ids = whelk.getSparqlQueryClient().queryIdsByForm(match);

                    var itemIds = slice(ids, offset, offset + limit);
                    var items = whelk.bulkLoad(itemIds)
                            .values()
                            .stream()
                            .map(doc -> makePreviewChangeSet(doc, diff))
                            .toList();

                    result.put("totalItems", ids.size());
                    result.put("changeSets", diff.getChangeSets());
                    result.put("items", items);
                }
            }

            // TODO support turtle etc?
            HttpTools.sendResponse(response, result, (String) MimeTypes.getJSONLD());
        } catch (Exception e) {
            HttpTools.sendError(response, HttpTools.mapError(e), e.getMessage(), e);
        }
    }

    // FIXME mangle the data in a more ergonomic way
    @SuppressWarnings("unchecked")
    private Map<?,?> makePreviewChangeSet(Document doc, FormDiff diff) {
        var modified = new ModifiedThing(doc.getThing(), diff, whelk.getJsonld().repeatableTerms);
        var beforeDoc = doc.clone();
        var afterDoc = doc.clone();
        ((List<Map<?,?>>) beforeDoc.data.get(JsonLd.GRAPH_KEY)).set(1, modified.getBefore());
        ((List<Map<?,?>>) afterDoc.data.get(JsonLd.GRAPH_KEY)).set(1, modified.getAfter());
        History history = new History(List.of(
                new DocumentVersion(beforeDoc, "", ""),
                new DocumentVersion(afterDoc, "", "")
        ), whelk.getJsonld());

        var result = history.m_changeSetsMap;
        result.put(JsonLd.ID_KEY, beforeDoc.getCompleteId());
        ((Map<String,Object>) DocumentUtil.getAtPath(result, List.of("changeSets", 0))).put("version", modified.getBefore());
        ((Map<String,Object>) DocumentUtil.getAtPath(result, List.of("changeSets", 1))).put("version", modified.getAfter());

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

    static int parsePositiveInt(HttpServletRequest request, String param, int defaultValue) {
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
}
