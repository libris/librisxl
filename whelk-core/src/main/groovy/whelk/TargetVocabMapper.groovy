package whelk

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j as Log

import trld.jsonld.Compaction
import trld.jsonld.Expansion
import trld.tvm.Mapmaker
import trld.tvm.Mapper

/**
 * Wrapper for the TRLD API (adjust as needed)
 */
@Log
@CompileStatic
class TargetVocabMapper {
    private Map<String, Map> targetVocabularyMaps = [:]
    private Object vocab
    private Map dataContext

    TargetVocabMapper(JsonLd jsonld, Map dataContext) {
        def vocabData = [
            (JsonLd.CONTEXT_KEY): jsonld.context,
            (JsonLd.GRAPH_KEY): jsonld.vocabIndex.values() as List
        ]
        vocab = Expansion.expand(vocabData, jsonld.vocabId)
        this.dataContext = (Map) (JsonLd.CONTEXT_KEY in dataContext ? dataContext[JsonLd.CONTEXT_KEY] : dataContext)
    }

    Object applyTargetVocabularyMap(String profileId, Map target, Map data) {
        Map targetMap = targetVocabularyMaps.get(profileId)
        if (targetMap == null) {
            targetMap = Mapmaker.makeTargetMap(vocab, target)
            targetVocabularyMaps.put(profileId, targetMap)
        }
        String dataIri = null
        boolean dropUnmapped = true
        if (!data.containsKey(JsonLd.CONTEXT_KEY)) {
            data[JsonLd.CONTEXT_KEY] = dataContext
        }
        List indata = Expansion.expand(data, dataIri)
        Object outdata = Mapper.mapTo(targetMap, indata, dropUnmapped)
        return Compaction.compact(target, outdata)
    }
}
