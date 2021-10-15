package whelk.external

interface Mapper {
    boolean mightHandle(String iri)
    Optional<Map> getThing(String iri)
    String datasetId()
}