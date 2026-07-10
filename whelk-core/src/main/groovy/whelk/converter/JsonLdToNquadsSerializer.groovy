package whelk.converter

import groovy.transform.CompileStatic

import trld.platform.Output
import trld.jsonld.Context
import static trld.jsonld.Expansion.expansion
import static trld.jsonld.Flattening.flatten
import static trld.jsonld.Rdf.toRdfDataset
import static trld.nq.Serializer.serialize

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
        data = ['@id': id, '@graph': data]
        var rdfDataset = toRdfDataset(flatten(expansion(context, null, data, id)))
        serialize(rdfDataset, out)
    }

}
