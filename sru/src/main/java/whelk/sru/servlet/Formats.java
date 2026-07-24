package whelk.sru.servlet;

import java.io.IOException;
import java.util.Map;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.Templates;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;

public class Formats {

   protected Map<Format, Xslt> transformers = null;
   private final TransformerFactory transformerFactory = TransformerFactory.newInstance();

   protected enum Format {
        MARC_XML,
        MODS,
        JSON,
        DC,
        REF_WORKS,
        UNSUPPORTED
    }

    protected static final Map<String, Format> FORMATS = Map.of(
            "marcxml", Format.MARC_XML,
            "json", Format.JSON,
            "mods", Format.MODS,
            "ris", Format.UNSUPPORTED,
            "dc", Format.DC,
            "rdfdc", Format.UNSUPPORTED,
            "bibtex", Format.UNSUPPORTED,
            "refworks", Format.REF_WORKS,
            "harvard", Format.UNSUPPORTED,
            "oxford", Format.UNSUPPORTED
    );

    public record Xslt(Templates templates, String contentType) {

    }

    public Xslt loadXslt(String name, String contentType) throws IOException, TransformerConfigurationException {
        var url = Thread.currentThread().getContextClassLoader().getResource(name);
        assert url != null;
        var xsltSource = new StreamSource(url.openStream(), url.toExternalForm());
        return new Xslt(transformerFactory.newTemplates(xsltSource), contentType);
    }

    public Formats() {
        try {
            transformers = Map.of(
                    Format.MODS, loadXslt("transformers/MARC21slim2MODS3.xsl", "text/xml"),
                    Format.DC, loadXslt("transformers/MARC21slim2DC.xsl", "text/xml"),
                    Format.REF_WORKS, loadXslt("transformers/refworks.xsl", "text/plain")
            );
        } catch (IOException | TransformerConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }
}
