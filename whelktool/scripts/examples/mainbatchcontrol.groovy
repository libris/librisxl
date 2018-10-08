selectByCollection('bib') {
    script('set-auth-role.groovy')(it)
    //script('remodel-termComponentList-bib.groovy')(it)
}

/*
selectByCollection('auth') {
    script('remodel-termComponentList-auth.groovy')(it)
}
*/