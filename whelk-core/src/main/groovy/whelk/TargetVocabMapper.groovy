package whelk

import groovy.util.logging.Log4j2 as Log

import trld.jsonld.Compaction
import trld.jsonld.Expansion
import trld.tvm.Mapmaker
import trld.tvm.Mapper

/**
 * Wrapper for the TRLD API (adjust as needed)
 */
@Log
class TargetVocabMapper {
    private Map targetVocabularyMaps = [:]
    private Object vocab
    private Map dataContext

    TargetVocabMapper(JsonLd jsonld, Map dataContext) {
        def vocabData = [
            (JsonLd.CONTEXT_KEY): jsonld.context,
            (JsonLd.GRAPH_KEY): jsonld.vocabIndex.values() as List
        ]
        vocab = Expansion.expand(vocabData, jsonld.vocabId)
        this.dataContext = JsonLd.CONTEXT_KEY in dataContext ? dataContext[JsonLd.CONTEXT_KEY] : dataContext
    }

    Object applyTargetVocabularyMap(String profileId, Map target, Map data) {
        Map targetMap = targetVocabularyMaps.get(profileId)
        if (targetMap == null) {
            targetMap = Mapmaker.makeTargetMap(vocab, target)
            targetVocabularyMaps.put(profileId, targetMap)
        }
        def dataIri = null
        def indata = data
        def dropUnmapped = true
        data[JsonLd.CONTEXT_KEY] = dataContext
        indata = Expansion.expand(indata, dataIri)
        Object outdata = Mapper.mapTo(targetMap, indata, dropUnmapped)
        return Compaction.compact(target, outdata)
    }
}
