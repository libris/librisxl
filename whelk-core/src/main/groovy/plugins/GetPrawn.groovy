package se.kb.libris.whelks.plugin;

/**
 * Prawn triggered when the whelk get()-method is called.
 */
abstract class GetPrawn extends BasicPlugin implements Prawn {
    public static final String TRIGGER = "get"
    public String getTrigger() {
        return TRIGGER
    }
}
