package whelk.plugin

import whelk.Document

abstract class BasicFilter extends BasicPlugin implements Filter {

    Document transmogrify(Document doc) {
        return filter(doc)
    }

    final Document filter(final Document document) {
        if (valid(document)) {
            return doFilter(document)
        }
        return document
    }

    final Map filter(final Map documentMap, String dataset) {
        return doFilter(documentMap, dataset)
    }

    abstract boolean valid(Document doc)
    abstract protected Document doFilter(final Document doc)
    abstract protected Map doFilter(final Map docMap, String dataset)
}
