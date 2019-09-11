package whelk

import com.google.common.base.Preconditions
import com.google.common.base.Strings

class Changer {
    private static String LIBRARY_PREFIX = 'https://libris.kb.se/library/'

    private String sigel
    private String scriptUri

    static Changer sigel(String sigel) {
        return new Changer(Preconditions.checkArgument(!Strings.isNullOrEmpty(sigel), 'Must set sigel'))
    }

    static Changer globalChange(String scriptUri, Optional<String> sigel) {
        return new Changer(
                Preconditions.checkArgument(!Strings.isNullOrEmpty(sigel.orElse('SEK')), 'Must set sigel'),
                Preconditions.checkArgument(!Strings.isNullOrEmpty(scriptUri), 'Must set scriptUri'))
    }

    @Deprecated
    static Changer sigelOrUnknown(String sigel) {
        return Strings.isNullOrEmpty(sigel) ? unknown() : sigel(sigel)
    }

    @Deprecated
    static Changer unknown() {
        return new Changer(null)
    }

    private Changer(String sigel) {
        this(sigel, null)
    }

    private Changer(String sigel, String scriptUri) {
        this.sigel = sigel
        this.scriptUri = scriptUri
    }

    void stampCreated(Document doc) {
        if (!isUnknown()) {
            doc.setDescriptionCreator(modifier())
            doc.setDescriptionLastModifier(modifier())
        }
    }

    void stampModified(Document doc, boolean minorUpdate, Date timestamp) {
        if (!minorUpdate && !isUnknown()) {
            doc.setDescriptionLastModifier(modifier())
        }

        if (isGlobalChange()) {
            doc.setGenerationDate(minorUpdate ? new Date() : timestamp)
            doc.setGenerationProcess(scriptUri)
        }
    }

    String getChangedBy () {
        if (isGlobalChange()) {
            return scriptUri
        }
        else {
            return sigel
        }
    }

    private String modifier() {
        LIBRARY_PREFIX + sigel
    }

    private boolean isGlobalChange() {
        scriptUri != null
    }

    private boolean isUnknown() {
        sigel == null
    }

    String toString() {
        if (isUnknown()) {
            return "UNKNOWN"
        }
        return "${getChangedBy()} (${modifier()})"
    }
}
