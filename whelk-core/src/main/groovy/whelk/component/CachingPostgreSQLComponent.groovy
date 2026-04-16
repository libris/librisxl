package whelk.component

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import groovy.util.logging.Log4j2 as Log
import whelk.Document

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

import static whelk.util.Jackson.mapper

@Log
class CachingPostgreSQLComponent extends PostgreSQLComponent {
    private static final int CARD_CACHE_MAX_SIZE = 250_000
    private static final int DOC_CACHE_MAX_SIZE = 250_000
    private static final int IRI_CACHE_MAX_SIZE = 500_000
    private LoadingCache<String, Map> cardCache
    private LoadingCache<String, byte[]> docCache
    private LoadingCache<String, String> iriToSystemIdCache

    CachingPostgreSQLComponent(Properties properties) {
        super(properties)
        initCaches()
    }

    @Override
    void logStats() {
        super.logStats()
        log.info("Card cache: ${cardCache.stats()}")
        log.info("Doc cache: ${docCache.stats()}")
        log.info("IRI->ID cache: ${iriToSystemIdCache.stats()}")
    }

    @Override
    Map<String, String> getSystemIdsByIris(Iterable iris) {
        Map<String, String> result = iriToSystemIdCache.getAll(iris)
        return result.findAll { k, v -> !v.isEmpty() }
    }

    @Override
    <T extends String> Iterable<Map> getCards(Iterable<T> iris) {
        cardCache.getAll(iris).values().findAll{ !it.isEmpty() }
    }

    @Override
    Map getCard(String iri) {
        return cardCache.get(iri)
    }

    Map<String, Document> cachedBulkLoad(Iterable<String> systemIds) {
        Map<String, byte[]> cached = docCache.getAll(systemIds)
        Map<String, Document> result = new TreeMap<>()
        cached.each { id, bytes ->
            if (bytes.length > 0) {
                result[id] = new Document(mapper.readValue(bytes, Map))
            }
        }
        return result
    }

    @Override
    protected boolean storeCard(CardEntry cardEntry, Connection connection) {
        boolean change = super.storeCard(cardEntry, connection)
        Document card = cardEntry.getCard()
        card.getThingIdentifiers().each { id ->
            cardCache.put(id, card.data)
        }

        return change
    }

    @Override
    protected void deleteCard(Document doc, Connection connection) {
        super.deleteCard(doc, connection)
        doc.getThingIdentifiers().each { id ->
            cardCache.invalidate(id)
        }
    }

    private Map superGetCard(String iri) {
        return super.getCard(iri)
    }

    private Map<String, String> loadIriMappingsFromDb(Iterable iris) {
        Map<String, String> ids = new HashMap<>()
        withDbConnection {
            Connection connection = getMyConnection()
            PreparedStatement stmt = null
            ResultSet rs = null
            try {
                stmt = connection.prepareStatement(GET_SYSTEMIDS_BY_IRIS)
                stmt.setArray(1, connection.createArrayOf("TEXT", iris as String[]))
                rs = stmt.executeQuery()
                while (rs.next()) {
                    ids.put(rs.getString(1), rs.getString(2))
                }
            } finally {
                if (rs != null) { try { rs.close() } catch (Exception ignore) {} }
                if (stmt != null) { try { stmt.close() } catch (Exception ignore) {} }
            }
        }
        return ids
    }

    private static final byte[] EMPTY_BYTES = new byte[0]

    private Map<String, Document> loadFromDb(Iterable<String> systemIds) {
        return withDbConnection {
            Connection connection = getMyConnection()
            PreparedStatement preparedStatement = null
            ResultSet rs = null
            try {
                preparedStatement = connection.prepareStatement(BULK_LOAD_DOCUMENTS)
                preparedStatement.setArray(1, connection.createArrayOf("TEXT", systemIds as String[]))
                rs = preparedStatement.executeQuery()
                Map<String, Document> result = new TreeMap<>()
                while (rs.next()) {
                    result[rs.getString("id")] = assembleDocument(rs)
                }
                return result
            } finally {
                if (rs != null) { try { rs.close() } catch (Exception ignore) {} }
                if (preparedStatement != null) { try { preparedStatement.close() } catch (Exception ignore) {} }
            }
        }
    }

    void initCaches() {
        cardCache = CacheBuilder.newBuilder()
                .maximumSize(CARD_CACHE_MAX_SIZE)
                .recordStats()
                .build(new CacheLoader<String, Map>() {
                    @Override
                    Map load(String iri) throws Exception {
                        return superGetCard(iri)
                    }

                    @Override
                    Map<String, Map> loadAll(Iterable<? extends String> iris) throws Exception {
                        def irisToIds = getSystemIdsByIris(iris)
                        def cards = createAndAddMissingCards(bulkLoadCards(irisToIds.values()))
                        cards.put('NON-EXISTING', Collections.emptyMap())
                        return iris.collectEntries { [it, cards.get(irisToIds.get(it) ?: "NON-EXISTING")] }
                    }
                })

        iriToSystemIdCache = CacheBuilder.newBuilder()
                .maximumSize(IRI_CACHE_MAX_SIZE)
                .recordStats()
                .build(new CacheLoader<String, String>() {
                    @Override
                    String load(String iri) throws Exception {
                        Map<String, String> result = loadIriMappingsFromDb([iri])
                        return result.containsKey(iri) ? result[iri] : ""
                    }

                    @Override
                    Map<String, String> loadAll(Iterable<? extends String> iris) throws Exception {
                        Map<String, String> result = loadIriMappingsFromDb(iris.collect())
                        Map<String, String> all = [:]
                        for (String iri : iris) {
                            all[iri] = result.containsKey(iri) ? result[iri] : ""
                        }
                        return all
                    }
                })

        docCache = CacheBuilder.newBuilder()
                .maximumSize(DOC_CACHE_MAX_SIZE)
                .recordStats()
                .build(new CacheLoader<String, byte[]>() {
                    @Override
                    byte[] load(String systemId) throws Exception {
                        Map<String, Document> loaded = loadFromDb([systemId])
                        Document doc = loaded[systemId]
                        if (doc == null || doc.deleted) {
                            return EMPTY_BYTES
                        }
                        return mapper.writeValueAsBytes(doc.data)
                    }

                    @Override
                    Map<String, byte[]> loadAll(Iterable<? extends String> systemIds) throws Exception {
                        Map<String, Document> loaded = loadFromDb(systemIds.collect())
                        Map<String, byte[]> result = [:]
                        for (String id : systemIds) {
                            Document doc = loaded[id]
                            if (doc == null || doc.deleted) {
                                result[id] = EMPTY_BYTES
                            } else {
                                result[id] = mapper.writeValueAsBytes(doc.data)
                            }
                        }
                        return result
                    }
                })
    }
}
