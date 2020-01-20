package whelk

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import whelk.component.PostgreSQLComponent
import whelk.exception.WhelkException

class Cards {
    private final int CACHE_MAX_SIZE = 200_000

    private Whelk whelk
    private PostgreSQLComponent storage
    private JsonLd jsonld

    private boolean useCache

    private LoadingCache<String, Map> cache

    Cards(Whelk whelk, boolean useCache = false) {
        this.storage = whelk.storage
        this.jsonld = whelk.jsonld
        this.useCache = useCache

        cache = CacheBuilder.newBuilder()
                .maximumSize(useCache ? CACHE_MAX_SIZE : 0)
                .recordStats()
                .build(loader())
    }

    Map getCard(String systemId) {
        return cache.get(systemId)
    }

    List<Map> getCards(Iterable<String> systemIds) {
        return cache.getAll(systemIds)
    }

    List<Map> getCardsByFollowingInCardRelations(List<String> startIris) {
        if (useCache) {
            return cache.getAll(storage.getIdsForEmbellish(startIris)).values().collect()
        } else {
            return addMissingCards(storage.getCardsForEmbellish(startIris)).values()
        }
    }

    void invalidate(String id) {
        cache.invalidate(id)
    }

    @Override
    String toString() {
        return cache.stats().toString()
    }

    private Map loadCard(String systemId) {
        storage.getCard(systemId) ?: makeCard(systemId)
    }

    private Map<String, Map> loadCards(Iterable<String> systemIds) {
        return addMissingCards(storage.bulkLoadCards(systemIds))
    }

    private Map<String, Map> addMissingCards(Map<String, Map> cards) {
        return cards.collectEntries { id, card ->
            [id, card ?: makeCard(id)]
        }
    }

    private Map makeCard(String systemId) {
        Document doc = whelk.getDocument(systemId)
        if (!doc) {
            throw new WhelkException("Could not find document with id " + systemId)
        }

        return jsonld.toCard(doc.data.false)
    }

    private CacheLoader<String, Map> loader() {
        return new CacheLoader<String, Map>() {
            @Override
            Map load(String key) throws Exception {
                return loadCard(key);
            }

            @Override
            Map<String, Map> loadAll(Iterable<? extends String> keys) throws Exception {
                return loadCards(keys)
            }
        }
    }
}
