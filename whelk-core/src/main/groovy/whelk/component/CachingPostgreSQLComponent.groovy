package whelk.component

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import groovy.util.logging.Log4j2 as Log
import whelk.Document

import java.sql.Connection

@Log
class CachingPostgreSQLComponent extends PostgreSQLComponent {
    private static final int CARD_CACHE_MAX_SIZE = 250_000
    private LoadingCache<String, Map> cardCache
    private LoadingCache<String, SortedSet<String>> dependencyCache

    CachingPostgreSQLComponent(Properties properties) {
        super(properties)
        initCaches()
    }

    @Override
    void logStats() {
        super.logStats()
        log.info("Card cache: ${cardCache.stats()}")
        log.info("Card dependency cache: ${dependencyCache.stats()}")
    }

    @Override
    Iterable<Map> getCardsForEmbellish(List<String> startIris) {
        return cardCache.getAll(getIdsForEmbellish(startIris)).values()
    }

    @Override
    Map getCard(String id) {
        return cardCache.get(id)
    }

    @Override
    protected SortedSet<String> getInCardDependers(String id) {
        return dependencyCache.get(id)
    }

    @Override
    protected boolean storeCard(CardEntry cardEntry, Connection connection) {
        boolean change = super.storeCard(cardEntry, connection)
        Document card = cardEntry.getCard()
        cardCache.put(card.getShortId(), card.data)
        return change
    }

    @Override
    protected void deleteCard(String systemId, Connection connection) {
        super.deleteCard(systemId, connection)
        cardCache.invalidate(systemId)
    }

    private SortedSet<String> superGetInCardDependers(String id) {
        return super.getInCardDependers(id)
    }

    private Map superGetCard(String id) {
        return super.getCard(id)
    }

    void initCaches() {
        cardCache = CacheBuilder.newBuilder()
                .maximumSize(CARD_CACHE_MAX_SIZE)
                .recordStats()
                .build(new CacheLoader<String, Map>() {
                    @Override
                    Map load(String systemId) throws Exception {
                        return superGetCard(systemId)
                    }

                    @Override
                    Map<String, Map> loadAll(Iterable<? extends String> systemIds) throws Exception {
                        return addMissingCards(bulkLoadCards(systemIds))
                    }
                })

        dependencyCache = CacheBuilder.newBuilder()
                .maximumSize(CARD_CACHE_MAX_SIZE)
                .recordStats()
                .build(new CacheLoader<String, SortedSet<String>>() {
                    @Override
                    SortedSet<String> load(String systemId) throws Exception {
                        return superGetInCardDependers(systemId)
                    }
                })
    }
}
