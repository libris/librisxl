package whelk

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import whelk.external.Wikidata

class External {
    private static final Map mappers = [
            'http://kblocalhost.kb.se:5000/v8h8lr6js3cmfvvd#it' : new Wikidata(),
    ]

    private static final int CACHE_SIZE = 50_000

    private LoadingCache<String, Optional<Document>> cache = CacheBuilder.newBuilder()
            .maximumSize(CACHE_SIZE)
            .recordStats()
            .build(new CacheLoader<String, Optional<Document>>() {
                @Override
                Optional<Document> load(String iri) throws Exception {
                    return getInternal(iri)
                }
            })
    
    Optional<Document> get(String iri) {
        cache.get(iri).map{ it.clone() }
    }

    Optional<Document> getEphemeral(String iri) {
        get(iri).map {doc ->
            doc.setRecordId("${doc.getThingIdentifiers().first()}#record".toString())
            doc
        }
    }

    private static Optional<Document> getInternal(String iri) {
        Document d = mappers.findResult { dataset, mapper ->
            mapper.getThing(iri).map{ document(it, JsonLd.CACHE_RECORD_TYPE, dataset) }.orElse(null)
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
}
