package whelk.search2;

import whelk.JsonLd;
import whelk.search2.querytree.QueryTree;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static whelk.search2.QueryUtil.makeFindUrlNoOffset;
import static whelk.search2.QueryUtil.makeFindUrlWithOffset;

public class Pagination {
    public static Map<String, Map<String, String>> makeLinks(int numHits, int maxItems, QueryTree queryTree, QueryParams queryParams) {
        if (queryParams.limit == 0) {
            // we don't have anything to paginate over
            return Collections.emptyMap();
        }

        var result = new LinkedHashMap<String, Map<String, String>>();

        Offsets offsets = new Offsets(Math.min(numHits, maxItems), queryParams.limit, queryParams.offset);

        result.put("first", Map.of(JsonLd.ID_KEY, makeFindUrlNoOffset(queryTree, queryParams)));
        result.put("last", Map.of(JsonLd.ID_KEY, makeFindUrlWithOffset(queryTree, queryParams, offsets.last)));

        if (offsets.prev != null) {
            if (offsets.prev == 0) {
                result.put("previous", result.get("first"));
            } else {
                result.put("previous", Map.of(JsonLd.ID_KEY, makeFindUrlWithOffset(queryTree, queryParams, offsets.prev)));
            }
        }

        if (offsets.next != null) {
            result.put("next", Map.of(JsonLd.ID_KEY, makeFindUrlWithOffset(queryTree, queryParams, offsets.next)));
        }

        return result;
    }

    private static class Offsets {
        Integer prev;
        Integer next;
        Integer last;

        Offsets(int total, int limit, int offset) throws IllegalArgumentException {
            if (limit <= 0) {
                throw new IllegalArgumentException("\"limit\" must be greater than 0.");
            }

            if (offset < 0) {
                throw new IllegalArgumentException("\"offset\" can't be negative.");
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

            if ((offset + limit) >= total) {
                this.last = offset;
            } else {
                if (total % limit == 0) {
                    this.last = total - limit;
                } else {
                    this.last = total - (total % limit);
                }
            }
        }
    }
}
