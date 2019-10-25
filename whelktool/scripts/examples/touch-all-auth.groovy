selectByCollection('auth') { auth ->
    try {
        auth.scheduleSave()
    }
    catch(Exception e) {
        println(e)
        e.printStackTrace()
    }
}
