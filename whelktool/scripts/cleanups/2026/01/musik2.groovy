import whelk.Whelk

Whelk whelk = getWhelk()

for (String id : ['pvt0plg0r2tmw9n1', 'fbr4fb6qh08ngbz7']) {
    whelk.bulkIndex(whelk.storage.getDependers(id))
}