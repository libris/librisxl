HIDDENVALUE = 'marc:hiddenValue'

Map remakeHiddenIdentifier(idStruct, hiddenValue) {
    int parenIndex = hiddenValue.indexOf('(')
    String value = parenIndex > -1
        ? hiddenValue.substring(0, parenIndex).trim()
        : hiddenValue
    int lastParenIndex = hiddenValue.lastIndexOf(')')
    String qualifier = lastParenIndex > -1
        ? hiddenValue.substring(parenIndex + 1, lastParenIndex).trim()
        : null
    Map remadeId = [(TYPE): idStruct[TYPE], value: (value.trim())]
    if (!idStruct.value && idStruct.qualifier) {
        remadeId.qualifier = idStruct.qualifier
    }
    if (qualifier) {
        if (remadeId.qualifier instanceof List)
            remadeId.qualifier << qualifier
        else
            remadeId.qualifier = qualifier
    }
    return remadeId
}

List asList(o) {
    return (o instanceof List) ? (List) o : o != null ? [o] : []
}

selectBySqlWhere(''' data::text LIKE '%"marc:hiddenValue"%' ''') { data ->
    def (record, instance) = data.graph
    if (!isInstanceOf(instance, 'Instance'))
        return

    def ids = instance.identifiedBy
    if (!ids)
        return

    def idsWithHidden = ids.findAll {
        HIDDENVALUE in it
    }
    if (!idsWithHidden)
        return

    def otherIds = instance.get('indirectlyIdentifiedBy', [])

    idsWithHidden.each { idStruct ->
        asList(idStruct.remove(HIDDENVALUE)).each {
            def remadeId = remakeHiddenIdentifier(idStruct, it)
            otherIds << remadeId
        }
        if (!idStruct.value) {
            ids.remove(idStruct)
        }
        if (!instance.identifiedBy) {
            instance.remove('identifiedBy')
        }
        data.scheduleSave(false)
    }
}
