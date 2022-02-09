package whelk.external

import groovy.util.logging.Log4j2 as Log

@Log
class TvmMapper implements Mapper{
    @Override
    boolean mightHandle(String iri) {
        return isTargetVocab(iri)
    }

    @Override
    Optional<Map> getThing(String iri) {
        if (!isTargetVocab(iri)) {
            return Optional.empty()
        }

        TvmEntity lcEntity = new TvmEntity(iri)
        return Optional.ofNullable((lcEntity.convert()))
    }

    @Override
    String datasetId() {
        return null
    }

    static boolean isTargetVocab(String iri) {
        isLoc(iri) // || is()...
    }

    private static boolean isLoc(String iri) {
        iri.startsWith("https://id.loc.gov") || iri.startsWith("https://id.loc.gov")
    }
}

class TvmEntity {
    static final String LOC_GF_NS = "https://id.loc.gov/authorities/genreForms/"

    String entityIri
    String shortId

    TvmEntity(String iri) {
        this.shortId = getShortId(iri)
        this.entityIri = LOC_GF_NS + shortId
    }

    Map convert() {
        return convertGf()
    }

    Map convertGf() {
        //        TODO: Map with TVM
        Map gf =
                [
                        '@id'  : entityIri,
                        '@type': "GenreForm"
                ]
        gf['prefLabel'] = "t.ex. Aeolian Harp"

        return gf
    }

    String getShortId(String iri) {
        iri.replaceAll(/.*\//, '')
    }
}