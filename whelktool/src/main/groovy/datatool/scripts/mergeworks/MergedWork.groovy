package datatool.scripts.mergeworks

import whelk.Document

interface MergedWork {
    Document doc
    Collection<Doc> derivedFrom
    File reportDir
}