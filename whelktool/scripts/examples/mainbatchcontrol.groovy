selectByCollection('bib') {
    script('set-auth-role.groovy')(it)
    //script('make-termComponentList-subdivision.groovy')(it)
}
