package whelk.filter

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.component.DocumentNormalizer

import java.sql.Connection

@Log
class NormalizerChain implements DocumentNormalizer{
    Collection<DocumentNormalizer> normalizers

    NormalizerChain(Collection<DocumentNormalizer> normalizers) {
        this.normalizers = normalizers.collect()
    }

    @Override
    void normalize(Document doc, Connection connection) {
        normalizers.each { n ->
            try {
                n.normalize(doc, connection)
            }
            catch (Exception e) {
                log.warn("Failed to normalize ${doc.shortId}: $e", e)
            }
        }
    }
}
