package whelk.external

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import whelk.Document
import whelk.IdGenerator
import whelk.JsonLd
import whelk.Whelk
import whelk.util.Metrics

class ExternalEntities {
    private final List<Mapper> mappers

    private static final int CACHE_SIZE = 10_000
    private final Set<String> bannedImports
    
    private LoadingCache<String, Optional<Document>> cache = CacheBuilder.newBuilder()
            .maximumSize(CACHE_SIZE)
            .recordStats()
            .build(new CacheLoader<String, Optional<Document>>() {
                @Override
                Optional<Document> load(String iri) throws Exception {
                    return getInternal(iri)
                }
            })

    ExternalEntities(Whelk whelk) {
        Map countryMappings = loadCountryMappings(whelk)
        mappers = [
                new Wikidata(countryMappings),
        ]
        bannedImports = Collections.unmodifiableSet(countryMappings.keySet())
        
        Metrics.cacheMetrics.addCache('external-entities', cache)
    }
    
    Optional<Document> get(String iri) {
        if (mappers.any { it.mightHandle(iri) }) {
            cache.get(iri).map{ it.clone() }
        }
        else {
            Optional.empty()
        }
    }

    Optional<Document> getEphemeral(String iri) {
        get(iri).map {doc ->
            doc.setRecordId("${doc.getThingIdentifiers().first()}#record".toString())
            doc.setRecordType(JsonLd.PLACEHOLDER_RECORD_TYPE)
            doc
        }
    }
    
    Set<String> getBannedImports() {
        return bannedImports
    }

    private Optional<Document> getInternal(String iri) {
        Document d = mappers.findResult { mapper ->
            mapper.getThing(iri).map{ document(it, JsonLd.CACHE_RECORD_TYPE, mapper.datasetId()) }.orElse(null)
        }

        return Optional.ofNullable (d)
    }

    static Document getPlaceholder(String iri) {
        def thing = [
                '@id'  : iri,
                '@type': JsonLd.PLACEHOLDER_ENTITY_TYPE
        ]

        document(thing, JsonLd.PLACEHOLDER_RECORD_TYPE)
    }

    private static Document document(Map thing, String recordType, String dataset = null) {
        def record = [
                '@id'       : Document.BASE_URI.toString() + IdGenerator.generate(),
                '@type'     : recordType,
                'mainEntity': ['@id': thing.'@id']
        ]
        
        if (dataset) {
            record.inDataset = ['@id': dataset]
        }

        new Document([
                '@graph': [
                        record,
                        thing
                ]
        ])
    }
    
    private static Map<String, String> loadCountryMappings(Whelk whelk) {
        if (!whelk.elasticFind) {
            return [:]
        }

        def query = [
                (JsonLd.TYPE_KEY): ['Country'],
                "q"              : ["*"],
                '_sort'          : [JsonLd.ID_KEY]
        ]
        
        Map<String, String> result = [:]
        def recordIds = whelk.elasticFind.findIds(query).collect{ whelk.baseUri.toString() + it }
        whelk.bulkLoad(recordIds).collect { id, doc ->
            JsonLd.asList(doc.getThing()['exactMatch']).each { match -> 
                result[(String) match[JsonLd.ID_KEY]] = doc.getThingIdentifiers().first() 
            }
        }
        return result
    }
}
