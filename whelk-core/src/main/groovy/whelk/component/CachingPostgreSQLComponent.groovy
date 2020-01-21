package whelk.component

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import groovy.util.logging.Log4j2 as Log
import whelk.Document

import java.sql.Connection

@Log
class CachingPostgreSQLComponent extends PostgreSQLComponent {
    private static final int CARD_CACHE_MAX_SIZE = 200_000
    private LoadingCache<String, Map> cardCache

    CachingPostgreSQLComponent(Properties properties) {
        super(properties)
        initCardCache()
    }

    @Override
    void logStats() {
        log.info("Card cache: ${cardCache.stats()}")
    }

    @Override
    Iterable<Map> getCardsByFollowingInCardRelations(List<String> startIris) {
        return cardCache.getAll(getIdsForEmbellish(startIris)).values()
    }

    @Override
    Map getCard(String id) {
        return cardCache.get(id)
    }

    @Override
    protected void storeCard(Document card, Connection connection) {
        super.storeCard(card, connection)
        cardCache.put(card.getShortId(), card.data)
    }

    @Override
    protected void deleteCard(Document card, Connection connection) {
        super.deleteCard(card, connection)
        cardCache.invalidate(card.getShortId())
    }

    void initCardCache() {
        cardCache = CacheBuilder.newBuilder()
                .maximumSize(CARD_CACHE_MAX_SIZE)
                .recordStats()
                .build(new CacheLoader<String, Map>() {
                    @Override
                    Map load(String systemId) throws Exception {
                        return CachingPostgreSQLComponent.super.getCard(systemId);
                    }

                    @Override
                    Map<String, Map> loadAll(Iterable<? extends String> systemIds) throws Exception {
                        return addMissingCards(bulkLoadCards(systemIds))
                    }
                })
    }
}
