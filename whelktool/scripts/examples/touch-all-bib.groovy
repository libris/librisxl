selectByCollection('bib') { bib ->
    try {
        bib.scheduleSave()
    }
    catch(Exception e) {
        println(e)
        e.printStackTrace()
    }
}
