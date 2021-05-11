/**
 * Removes 541 and 583 from _marcUncompleted in holds.
 *
 * See LXL-3583 for more info.
 */
where = "collection = 'hold' AND deleted = false AND data#>>'{@graph,0,_marcUncompleted}' ~ '\"541\"|\"583\"'"

selectBySqlWhere(where) { data ->
    List marcUncompleted = asList(data.graph[0]._marcUncompleted)
    boolean modified = false

    marcUncompleted.removeAll {
        if ('541' in it || '583' in it) {
            modified = true
        }
    }

    if (!marcUncompleted)
        data.graph[0].remove('_marcUncompleted')

    if (modified)
        data.scheduleSave()
}

private static List asList(o) {
    return (o instanceof List) ? (List) o : o != null ? [o] : []
}
