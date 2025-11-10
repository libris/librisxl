import whelk.Whelk
import whelk.datatool.DocumentItem

Whelk whelk = getWhelk()

selectByCollection('auth') { DocumentItem doc ->
    whelk.storage.recalculateDependencies(doc.doc)
}