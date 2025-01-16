package whelk.datatool.util;

import whelk.JsonLd;
import whelk.component.PostgreSQLComponent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static whelk.datatool.WhelkTool.DEFAULT_FETCH_SIZE;

public class IdLoader {
    public record Id(String shortId, String thingIri, String recordIri) {
    }

    private final PostgreSQLComponent storage;

    public IdLoader(PostgreSQLComponent storage) {
        this.storage = storage;
    }

    public static List<String> fromFile(String fileLocation) throws URISyntaxException, IOException {
        return new BufferedReader(new InputStreamReader(new URI(fileLocation).toURL().openStream()))
                .lines()
                .map(String::trim)
                .toList();
    }

    public List<Id> loadAllIds(Collection<String> shortIds) {
        String q = """
                SELECT id, data#>>'{@graph,1,@id}' AS thingIri, data#>>'{@graph,0,@id}' AS recordIri
                FROM lddb
                WHERE id = ANY(?)
                """;

        Function<ResultSet, List<Id>> collectResults = (rs) -> {
            List<Id> ids = new ArrayList<>();
            try {
                while (rs.next()) {
                    ids.add(new Id(rs.getString("id"), rs.getString("thingIri"), rs.getString("recordIri")));
                }
            } catch (SQLException e) {
                return Collections.emptyList();
            }
            return ids;
        };

        try {
            return query(q, collectResults, Map.of(1, shortIds));
        } catch (SQLException ignored) {
            return Collections.emptyList();
        }
    }

    public List<String> collectXlShortIds(Collection<String> ids) {
        Map<String, String> iriToShortId = findShortIdsForIris(ids.stream().filter(JsonLd::looksLikeIri).toList());
        return ids.stream()
                .map(id -> iriToShortId.getOrDefault(id, isXlShortId(id) ? id : null))
                .filter(Objects::nonNull)
                .toList();
    }

    public List<String> collectXlShortIds(Collection<String> ids, String marcCollection) {
        if (!Arrays.asList("bib", "auth", "hold").contains(marcCollection)) {
            return collectXlShortIds(ids);
        }
        Map<String, String> iriToShortId = findShortIdsForIris(ids.stream().filter(JsonLd::looksLikeIri).toList());
        Map<String, String> voyagerIdToXlShortId = findXlShortIdsForVoyagerIds(
                ids.stream().filter(IdLoader::isVoyagerId).toList(),
                marcCollection);
        return ids.stream()
                .map(id -> iriToShortId.getOrDefault(id, voyagerIdToXlShortId.getOrDefault(id, isXlShortId(id) ? id : null)))
                .filter(Objects::nonNull)
                .toList();
    }

    public static boolean isXlShortId(String id) {
        return id.matches("[0-9a-z]{15,17}");
    }

    private Map<String, String> findXlShortIdsForVoyagerIds(Collection<String> voyagerIds, String marcCollection) {
        if (voyagerIds.isEmpty()) {
            return Collections.emptyMap();
        }

        String q = """
                SELECT id, data#>>'{@graph,0,controlNumber}' AS controlNumber
                FROM lddb
                WHERE collection = ? AND data#>>'{@graph,0,controlNumber}' = ANY(?)
                """;

        Function<ResultSet, Map<String, String>> collectResults = (rs) -> {
            Map<String, String> voyagerIdToXlId = new HashMap<>();
            try {
                while (rs.next()) {
                    voyagerIdToXlId.put(rs.getString("controlNumber"), rs.getString("id"));
                }
            } catch (SQLException e) {
                return Collections.emptyMap();
            }
            return voyagerIdToXlId;
        };

        try {
            return query(q, collectResults, Map.of(1, marcCollection, 2, voyagerIds));
        } catch (SQLException ignored) {
            return Collections.emptyMap();
        }
    }

    private static boolean isVoyagerId(String id) {
        return id.matches("[0-9]{1,13}");
    }

    private Map<String, String> findShortIdsForIris(Collection<String> iris) {
        if (iris.isEmpty()) {
            return Collections.emptyMap();
        }
        String q = """
                SELECT id, iri
                FROM lddb__identifiers
                WHERE iri = ANY(?)
                """;

        Function<ResultSet, Map<String, String>> collectResults = (rs) -> {
            Map<String, String> iriToShortId = new HashMap<>();
            try {
                while (rs.next()) {
                    iriToShortId.put(rs.getString("iri"), rs.getString("id"));
                }
            } catch (SQLException e) {
                return Collections.emptyMap();
            }
            return iriToShortId;
        };

        try {
            return query(q, collectResults, Map.of(1, iris));
        } catch (SQLException ignored) {
            return Collections.emptyMap();
        }
    }

    private <T> T query(String query, Function<ResultSet, T> collectResults, Map<Integer, Object> params) throws SQLException {
        Connection conn = storage.getOuterConnection();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        T results;
        try {
            conn.setAutoCommit(false);
            stmt = conn.prepareStatement(query);
            for (var e : params.entrySet()) {
                int i = e.getKey();
                var obj = e.getValue();
                if (obj instanceof Collection) {
                    String[] values = ((Collection<?>) obj).stream().map(String.class::cast).toArray(String[]::new);
                    stmt.setArray(i, conn.createArrayOf("TEXT", values));
                } else if (obj instanceof String) {
                    stmt.setString(i, (String) obj);
                }
            }
            stmt.setFetchSize(DEFAULT_FETCH_SIZE);
            rs = stmt.executeQuery();
            results = collectResults.apply(rs);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
        return results;
    }
}
