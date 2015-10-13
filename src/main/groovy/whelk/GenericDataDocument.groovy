package whelk


/**
 * Created by markus on 2015-10-13.
 */
class GenericDataDocument extends Document {

    String data

    GenericDataDocument(String id, String data, Map manifest) {
        this.id = id
        this.data = data
        this.manifest = deepCopy(manifest)
    }

    boolean isJson() { false }
}
