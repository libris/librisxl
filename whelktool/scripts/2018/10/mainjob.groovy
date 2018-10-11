selectByCollection('bib') {
    script('fix-unbalanced-brackets.groovy')(it)
    script('remodel-termComponentList.groovy')(it)
}

selectByCollection('auth') {
    script('remodel-termComponentList.groovy')(it)
}
