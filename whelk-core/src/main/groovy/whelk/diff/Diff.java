package whelk.diff;

import java.io.IOException;
import java.util.*;

import static whelk.util.Jackson.mapper;

public class Diff {

    public static List diff(String json_a, String json_b) throws IOException {
        Map a = mapper.readValue(json_a, Map.class);
        Map b = mapper.readValue(json_b, Map.class);

        return diff(a, b);
    }

    public static List diff(Map a, Map b) throws IOException {
        ArrayList path = new ArrayList();
        ArrayList result = new ArrayList();
        diffInternal(a, b, path, result);
        return result;
    }

    private static void diffInternal(Object oA, Object oB, List path, List result) {

        if (!oA.getClass().equals(oB.getClass())) {
            result.add(Map.of("op", "replace", "path", path, "value", oB));
            return;
        }

        if (oA instanceof Map a) {
            Map b = (Map) oB;
            Set<Object> aKeys = a.keySet();
            Set<Object> bKeys = b.keySet();

            Set missing = new HashSet(aKeys);
            missing.removeAll(bKeys);

            Set added = new HashSet(bKeys);
            added.removeAll(aKeys);

            Set remaining = new HashSet(aKeys);
            remaining.retainAll(bKeys);

            if (!added.isEmpty()) {
                for (Object key : added) {
                    ArrayList addedPath = new ArrayList(path);
                    addedPath.add(key);
                    result.add(Map.of("op", "add", "path", addedPath, "value", b.get(key)));
                }
            }

            if (!missing.isEmpty()) {
                for (Object key : missing) {
                    ArrayList missingPath = new ArrayList(path);
                    missingPath.add(key);
                    result.add(Map.of("op", "remove", "path", missingPath));
                }
            }

            for (Object key : remaining) {
                List nextPath = new ArrayList(path);
                nextPath.add(key);
                diffInternal(a.get(key), b.get(key), nextPath, result);
                return;
            }
        } else if (oA instanceof List a) {
            List b = (List) oB;

            int countDiff = b.size() - a.size();
            if (countDiff > 0) {
                for (int i = a.size(); i < b.size(); ++i) {
                    List addedPath = new ArrayList(path);
                    addedPath.add(i);
                    result.add(Map.of("op", "add", "path", addedPath, "value", b.get(i)));
                }
            } else if (countDiff < 0) {
                for (int i = b.size(); i < a.size(); ++i) {
                    List missingPath = new ArrayList(path);
                    missingPath.add(i);
                    result.add(Map.of("op", "remove", "path", missingPath));
                }
            }

            int count = Integer.min( a.size(), b.size());
            for (int i = 0; i < count; ++i) {
                List nextPath = new ArrayList(path);
                nextPath.add(i);
                diffInternal(a.get(i), b.get(i), nextPath, result);
                return;
            }
        } else { // String, Integer, Boolean etc
            if ( !oA.equals(oB) ) {
                result.add(Map.of("op", "replace", "path", path, "value", oB));
            }
        }
    }
}
