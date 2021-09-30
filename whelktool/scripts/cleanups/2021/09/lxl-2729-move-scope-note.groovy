selectByCollection('auth') { data ->
    def thing = data.graph[1]
    def hasNote = thing.hasNote

    if (!isInstanceOf(thing, "Concept")) {
        return
    }

    if (!hasNote || !hasNote.scopeNote) {
        return
    }

    hasNote.each {
        if (it.scopeNote) {
            thing['scopeNote'] = thing['scopeNote'] ?: []
            thing['scopeNote'].addAll(it.scopeNote)
        }
    }

    boolean saveMe = hasNote.removeAll { it.scopeNote != null }

    if (hasNote.isEmpty()) {
        thing.remove('hasNote')
    }

    if (saveMe) {
        data.scheduleSave()
    }
}