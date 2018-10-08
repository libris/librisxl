selectByCollection('bib') {
    script('fix-unbalanced-brackets.groovy')(it)
    script('remodel-termComponentList-bib.groovy')(it)
}

selectByCollection('auth') {
    script('remodel-termComponentList-auth.groovy')(it)
}
