selectByCollection('bib') {
    script('remodel-termComponentList-bib.groovy')(it)
}

selectByCollection('auth') {
    script('remodel-termComponentList-auth.groovy')(it)
}
