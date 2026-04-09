package whelk;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.jspecify.annotations.NullMarked;
import whelk.util.Metrics;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static whelk.search2.QueryUtil.castToStringObjectMap;

public class ChipCache {
    private static final int CACHE_SIZE = 5_000;
    private static final int EXPIRE_MINS = 10;

    private final LoadingCache<String, Map<String, Object>> cache;

    ChipCache(Whelk whelk) {
        cache = CacheBuilder.newBuilder()
                .maximumSize(CACHE_SIZE)
                .expireAfterWrite(Duration.ofMinutes(EXPIRE_MINS))
                .recordStats()
                .build(buildLoader(whelk));

        Metrics.cacheMetrics.addCache("chipCache", cache);
    }

    public Map<String, Map<String, Object>> getChips(Iterable<String> iris) {
        try {
            return cache.getAll(iris);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private CacheLoader<String, Map<String, Object>> buildLoader(Whelk whelk) {
        return new CacheLoader<>() {

            @Override
            @NullMarked
            public Map<String, Object> load(String iri) {
                return loadAll(List.of(iri)).get(iri);
            }

            @Override
            @NullMarked
            public Map<String, Map<String, Object>> loadAll(Iterable<? extends String> iris) {
                var cards = whelk.getCards(iris);

                var chips = new HashMap<String, Map<String, Object>>();
                for (var iri : iris) {
                    var cardGraph = cards.get(iri);
                    if (cardGraph != null) {
                        var chip = castToStringObjectMap(whelk.getJsonld().toChip(new Document(cardGraph).getThing()));
                        // make top-level immutable to guard from accidental modification
                        chips.put(iri, Map.copyOf(chip));
                    } else {
                        chips.put(iri, dummyChip(iri));
                    }
                }
                return chips;
            }

            private static Map<String, Object> dummyChip(String id) {
                return Map.of(
                        JsonLd.ID_KEY, id,
                        JsonLd.Rdfs.LABEL, id
                );
            }
        };
    }
}
