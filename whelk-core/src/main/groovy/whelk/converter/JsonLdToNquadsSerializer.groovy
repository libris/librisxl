package whelk.converter

import groovy.transform.CompileStatic

import org.apache.commons.codec.digest.DigestUtils

import trld.platform.Output
import trld.jsonld.Context
import trld.jsonld.Expansion
import trld.jsonld.Flattening
import trld.jsonld.BNodes
import trld.jsonld.Rdf
import trld.jsonld.RdfDataset

@CompileStatic
class JsonLdToNquadsSerializer implements JsonLdToRdfSerializer {

    Context context
    Output out

    JsonLdToNquadsSerializer(Map ctx, OutputStream ostream) {
      this.context = new Context(null, null, null).getContext(ctx, null)
      setOutputStream(ostream)
    }

    void setOutputStream(OutputStream ostream) {
        out = new Output(new PrintStream(ostream))
    }

    void prelude() {
    }

    void writeGraph(String id, Object data) {
        // Note: not enough:
        // var rdfDataset = toRdfDataset(flatten(expansion(context, null, data, id)))
        // ; need to ensure unique part bnode IDs in resulting output dataset:
        var hash = DigestUtils.sha256Hex(id)
        var pfx = "_:${hash}-" as String
        var bnodes = new BNodes() {
           String makeBnodeId(String identifier) {
            return pfx + super.makeBnodeId(identifier).substring(2)
           }
        }
        data = ['@id': id, '@graph': data] // to named graph
        var flatExpandedData = Flattening.flatten(Expansion.expansion(context, null, data, id))
        Map<String, Map<String, Object>> nodeMap = ['@default': [:]]
        Flattening.makeNodeMap(bnodes, flatExpandedData, nodeMap)
        var rdfDataset = new RdfDataset()
        Rdf.jsonldToRdfDataset(nodeMap, rdfDataset, bnodes, null)

        trld.nq.Serializer.serialize(rdfDataset, out)
    }

}
