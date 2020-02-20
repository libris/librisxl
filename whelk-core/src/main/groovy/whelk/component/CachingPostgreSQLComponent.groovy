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

    CachingPostgreSQLComponent(Properties properties) {
        super(properties)
        initCaches()
    }

    @Override
    void logStats() {
        super.logStats()
        log.info("Card cache: ${cardCache.stats()}")
    }

    @Override
    Iterable<Map> getCards(Iterable<String> iris) {
        cardCache.getAll(iris).values().findAll{ !it.isEmpty() }
    }

    @Override
    Map getCard(String iri) {
        return cardCache.get(iri)
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
    }
}
