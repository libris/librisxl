package whelk.housekeeping;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.jetbrains.annotations.NotNull;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.datatool.RecordedChange;
import whelk.datatool.bulkchange.BulkJobDocument;
import whelk.datatool.bulkchange.BulkPreviewJob;
import whelk.datatool.bulkchange.Specification;
import whelk.history.DocumentVersion;
import whelk.history.History;
import whelk.util.DocumentUtil;
import whelk.util.Offsets;
import whelk.util.WhelkFactory;
import whelk.util.http.BadRequestException;
import whelk.util.http.HttpTools;
import whelk.util.http.MimeTypes;
import whelk.util.http.NotFoundException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static whelk.JsonLd.GRAPH_KEY;
import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.RECORD_KEY;
import static whelk.datatool.bulkchange.BulkPreviewJob.RECORD_MAX_ITEMS;
import static whelk.util.Unicode.stripPrefix;

public class BulkChangePreviewAPI extends HttpServlet {

    private static final String BULK_CHANGE_PREVIEW_TYPE = "bulk:Preview";
    private static final String PREVIEW_API_PATH = "/_bulk-change/preview";
    private static final int DEFAULT_LIMIT = 1;
    private static final ThreadGroup threadGroup = new ThreadGroup(BulkChangeRunner.class.getSimpleName());
    private static final int TIMEOUT_MS = 7000;

    private static final int CACHE_SIZE = 20;
    private static final int CACHE_TIMEOUT_MINUTES = 10;

    private Whelk whelk;

    private final LoadingCache<String, Preview> cache = CacheBuilder.newBuilder()
            .maximumSize(CACHE_SIZE)
            .expireAfterAccess(CACHE_TIMEOUT_MINUTES, TimeUnit.MINUTES)
            .recordStats()
            .build(new CacheLoader<>() {
                @Override
                public @NotNull Preview load(@NotNull String id) {
                    return new Preview(id);
                }
            });

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
                throw new NotFoundException("Document not found");
            }

            int limit = nonNegativeInt(request, "_limit", DEFAULT_LIMIT);
            int offset = nonNegativeInt(request, "_offset", 0);

            var preview = cache.get(id);
            var changes = preview.getChanges(offset + limit + 1, TIMEOUT_MS);

            var result = makePreview(changes, offset, limit, id, preview.isFinished());

            var spec = preview.getSpec();
            if (spec instanceof Specification.Update) {
                result.put("changeSets", ((Specification.Update) spec).getTransform(whelk).getChangeSets());
            }

            // TODO support turtle etc?
            HttpTools.sendResponse(response, result, (String) MimeTypes.getJSONLD());
        } catch (Exception e) {
            HttpTools.sendError(response, HttpTools.mapError(e), e.getMessage(), e);
        }
    }

    private Map<Object, Object> makePreview(List<RecordedChange> changes, int offset, int limit, String id, boolean complete) {
        Map<Object, Object> result = new LinkedHashMap<>();
        result.put(JsonLd.TYPE_KEY, BULK_CHANGE_PREVIEW_TYPE);

        var totalItems = changes.size();

        var items = slice(changes, offset, Math.min(offset + limit, RECORD_MAX_ITEMS)).stream()
                .map(this::makePreviewChangeSet)
                .toList();

        Offsets offsets = new Offsets(Math.min(totalItems, RECORD_MAX_ITEMS), limit, offset);
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
        result.put("items", items);
        result.put("maxItems", RECORD_MAX_ITEMS);
        result.put("_complete", complete);

        return result;
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
    private Map<?,?> makePreviewChangeSet(RecordedChange recordChange) {
        Function<Document, Map<Object, Object>> getThing = (Document doc) -> doc != null ?
                new LinkedHashMap<Object, Object>(doc.getThing()) :
                new HashMap<>();
        Function<Document, Map<?, ?>> getRecord = (Document doc) -> doc != null ?
                new LinkedHashMap<Object, Object>(doc.getRecord()) :
                new HashMap<>();

        var thingBefore = getThing.apply(recordChange.before());
        var thingAfter = getThing.apply(recordChange.after());
        var recordBefore = getRecord.apply(recordChange.before());
        var recordAfter = getRecord.apply(recordChange.after());

        var recordCopy = new LinkedHashMap<>(recordBefore.isEmpty() ? recordAfter : recordBefore);

        // Remove @id from record to prevent it from being shown as a link in the diff view
        recordBefore.remove(ID_KEY);
        recordAfter.remove(ID_KEY);

        // We want to compute the diff for a "framed" thing
        thingBefore.put(RECORD_KEY, recordBefore);
        thingAfter.put(RECORD_KEY, recordAfter);

        // However when the diff is computed we need "@graph form", hence the same record copy at @graph,0 in both versions
        var beforeDoc = new Document(Map.of(GRAPH_KEY, List.of(recordCopy, thingBefore)));
        var afterDoc = new Document(Map.of(GRAPH_KEY, List.of(recordCopy, thingAfter)));
        var id = (String) recordCopy.get(ID_KEY);

        var result = getChangeSetsMap(beforeDoc, afterDoc, id);
        ((Map<String,Object>) DocumentUtil.getAtPath(result, List.of("changeSets", 0))).put("version",
                beforeDoc.getThing());
        ((Map<String,Object>) DocumentUtil.getAtPath(result, List.of("changeSets", 1))).put("version",
                afterDoc.getThing());

        return result;
    }

    @SuppressWarnings("unchecked")
    private Map<?, ?> getChangeSetsMap(Document before, Document after, String id) {
        var versions = List.of(
                new DocumentVersion(before, "", ""),
                new DocumentVersion(after, "", "")
        );
        History history = new History(versions, whelk.getJsonld());
        var result = history.m_changeSetsMap;
        result.put(ID_KEY, id);
        return result;
    }

    private static <T> List<T> slice(List<T> l, int from, int to) {
        int fromIx = Math.max(Math.min(from, l.size()), 0);
        int toIx = Math.min(to, l.size());
        return l.subList(fromIx, toIx);
    }

    private BulkJobDocument load(String systemId) {
        Document doc = whelk.getDocument(systemId);
        if (doc == null) {
            throw new NotFoundException("Document not found");
        }
        try {
            return new BulkJobDocument(doc.data);
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

    class Preview {
        String id;
        String systemId;
        BulkPreviewJob job;

        Preview(String id) {
            this.id = id;
            this.systemId = stripPrefix(id, Document.getBASE_URI().toString());
        }

        synchronized List<RecordedChange> getChanges(int minNumWanted, int timeoutMs) {
            var jobDoc = load(systemId);
            if (job!= null && !job.isSameVersion(jobDoc)) {
                job.cancel();
                job = null;
            }
            if (job == null) {
                try {
                    job = new BulkPreviewJob(whelk, id);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                new Thread(threadGroup, job).start();
            }

            long returnTime = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < returnTime) {
                var changes = job.getChanges();
                if (changes.size() < minNumWanted && !job.isFinished()) {
                    sleep();
                } else {
                    return changes;
                }
            }

            // TODO
            return job.getChanges();
        }

        // FIXME depends on getChanges being called before...
        synchronized boolean isFinished() {
            return job.isFinished();
        }

        Specification getSpec() {
            return load(systemId).getSpecification();
        }

        private void sleep() {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {}
        }
    }

}
