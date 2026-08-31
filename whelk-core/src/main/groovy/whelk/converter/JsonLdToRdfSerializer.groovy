package whelk.converter

import groovy.transform.CompileStatic

@CompileStatic
interface JsonLdToRdfSerializer {

    abstract void setOutputStream(OutputStream ostream)

    abstract void prelude()

    abstract void writeGraph(String id, Object data)

}
