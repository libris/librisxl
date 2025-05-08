package whelk.util.http;

import whelk.Whelk;
import whelk.util.WhelkFactory;

public class CoreWhelkHttpServlet extends WhelkHttpServlet {
    @Override
    protected Whelk createWhelk() {
        return WhelkFactory.getSingletonCoreWhelk();
    }
}
