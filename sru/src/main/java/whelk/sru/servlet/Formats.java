package whelk.sru.servlet;

import java.io.IOException;
import java.util.Map;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.Templates;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;

public class Formats {

   protected Map<Format, Templates> transformers = null;
   private final TransformerFactory transformerFactory = TransformerFactory.newInstance();

   protected enum Format {
        MARC_XML,
        MODS,
        JSON,
        DC,
        UNSUPPORTED,
    }

    protected static final Map<String, Format> FORMATS = Map.of(
            "marcxml", Format.MARC_XML,
            "json", Format.JSON,
            "mods", Format.MODS,
            "ris", Format.UNSUPPORTED,
            "dc", Format.DC,
            "rdfdc", Format.UNSUPPORTED,
            "bibtex", Format.UNSUPPORTED,
            "refworks", Format.UNSUPPORTED,
            "harvard", Format.UNSUPPORTED,
            "oxford", Format.UNSUPPORTED
    );

    private Templates loadXslt(String name) throws IOException, TransformerConfigurationException {
        var url = Thread.currentThread().getContextClassLoader().getResource(name);
        assert url != null;
        var xsltSource = new StreamSource(url.openStream(), url.toExternalForm());
        return transformerFactory.newTemplates(xsltSource);
    }

    public Formats() {
        try {
            transformers = Map.of(
                    Format.MODS, loadXslt("transformers/MARC21slim2MODS3.xsl"),
                    Format.DC, loadXslt("transformers/MARC21slim2DC.xsl")
            );
        } catch (IOException | TransformerConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }
}
