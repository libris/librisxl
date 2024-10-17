package whelk.datatool.util;

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
        String where = String.format("id in ( '%s' )", String.join("','", shortIds));
        String q = String.format("""
                SELECT id, data#>>'{@graph,1,@id}' AS thingIri, data#>>'{@graph,0,@id}' AS recordIri
                FROM lddb
                WHERE %s
                """, where);

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
            return query(q, collectResults);
        } catch (SQLException ignored) {
            return Collections.emptyList();
        }
    }

    public List<String> collectXlShortIds(Collection<String> xlIds) {
        Map<String, String> iriToShortId = findShortIdsForIris(xlIds.stream().filter(id -> id.contains(":")).toList());
        return xlIds.stream().map(id -> iriToShortId.getOrDefault(id, id)).toList();
    }

    public List<String> collectXlShortIds(Collection<String> ids, String marcCollection) {
        if (!Arrays.asList("bib", "auth", "hold").contains(marcCollection)) {
            return collectXlShortIds(ids);
        }
        Map<String, String> iriToShortId = findShortIdsForIris(ids.stream().filter(id -> id.contains(":")).toList());
        Map<String, String> voyagerIdToXlShortID = findXlShortIdsForVoyagerIds(
                ids.stream().filter(IdLoader::isVoyagerId).toList(),
                marcCollection);
        return ids.stream().map(id -> iriToShortId.getOrDefault(id, voyagerIdToXlShortID.getOrDefault(id, id))).toList();
    }

    private Map<String, String> findXlShortIdsForVoyagerIds(Collection<String> voyagerIds, String marcCollection) {
        if (voyagerIds.isEmpty()) {
            return Collections.emptyMap();
        }

        String where = String.format("collection = '%s' and data#>>'{@graph,0,controlNumber}' in ( '%s' )",
                marcCollection,
                String.join("','", voyagerIds));

        String q = String.format("""
                SELECT id, data#>>'{@graph,0,controlNumber}' AS controlNumber
                FROM lddb
                WHERE %s
                """, where);

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
            return query(q, collectResults);
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
        String q = String.format("""
                SELECT id, iri
                FROM lddb__identifiers
                WHERE iri IN ( '%s' )
                """, String.join("','", iris));

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
            return query(q, collectResults);
        } catch (SQLException ignored) {
            return Collections.emptyMap();
        }
    }

    private <T> T query(String query, Function<ResultSet, T> collectResults) throws SQLException {
        Connection conn = storage.getOuterConnection();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        T results;
        try {
            conn.setAutoCommit(false);
            stmt = conn.prepareStatement(query);
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
