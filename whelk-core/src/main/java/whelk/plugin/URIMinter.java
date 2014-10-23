package whelk.plugin;

import java.net.URI;
import whelk.Document;

public interface URIMinter extends Plugin {
    public URI mint(Document d);
}
