package whelk.converter;

import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.StartElement;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class XmlIterParser implements Iterator<String>, AutoCloseable {

    private final XMLEventReader reader;
    private final XMLOutputFactory outputFactory;
    private final XMLEventFactory eventFactory;

    private final List<Namespace> ancestorNamespaces = new ArrayList<>();
    private StartElement childStart = null;

    public XmlIterParser(InputStream is) throws Exception {
        var inputFactory = XMLInputFactory.newInstance();
        // Enable DTD entity support (fairly common in RDF/XML)
        inputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, true);
        // But no external (insecure!)
        inputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);

        this.reader = inputFactory.createXMLEventReader(is);
        this.outputFactory = XMLOutputFactory.newInstance();
        this.eventFactory = XMLEventFactory.newInstance();
    }

    @Override
    public boolean hasNext() {
        if (childStart == null) {
            try {
                while (reader.hasNext()) {
                    var event = reader.nextEvent();
                    // Ignore root element
                    if (event.isStartElement()) {
                        reader.nextEvent();
                        var start = event.asStartElement();
                        // Collect active namespaces
                        var nsIter = start.getNamespaces();
                        while (nsIter.hasNext()) {
                            ancestorNamespaces.add((Namespace) nsIter.next());
                        }
                        break;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (!reader.hasNext()) {
            return false;
        }

        try {
            while (reader.hasNext()) {
                var event = reader.peek();
                if (event.isStartElement()) {
                    childStart = event.asStartElement();
                    return true;
                }
                reader.nextEvent(); // Drop intermediate whitespace
            }
            return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private StartElement completeChildStart(StartElement childStart) {
        var allActiveNamespaces = new ArrayList<>(ancestorNamespaces);
        var childNsIter = childStart.getNamespaces();
        while (childNsIter.hasNext()) {
            var ns = (Namespace) childNsIter.next();
            // Override ancestor duplicates with local declarations
            allActiveNamespaces.removeIf(n -> n.getPrefix().equals(ns.getPrefix()));
            allActiveNamespaces.add(ns);
        }

        return eventFactory.createStartElement(
            childStart.getName(),
            childStart.getAttributes(),
            allActiveNamespaces.iterator()
        );
    }

    @Override
    public String next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        var sw = new StringWriter();
        XMLEventWriter writer;
        try {
            writer = outputFactory.createXMLEventWriter(sw);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (childStart == null) {
            throw new NoSuchElementException();
        }

        try {
            reader.nextEvent(); // consume
            childStart = completeChildStart(childStart);
            writer.add(childStart);

            var depth = 1;
            while (reader.hasNext() && depth > 0) {
                var event = reader.nextEvent();
                writer.add(event);

                if (event.isStartElement()) depth++;
                if (event.isEndElement()) depth--;
            }

            writer.close();
            return sw.toString();

        } catch (Exception e) {
            throw new RuntimeException("Collecting inner XML failed", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (reader != null) reader.close();
    }
}
