package whelk.camel

import org.apache.camel.Exchange
import org.apache.camel.Processor
import whelk.plugin.BasicPlugin

/**
 * Created by markus on 19/02/15.
 */
class XInfoRouteProcessor extends BasicPlugin implements Processor {

    private XInfoRouteProcessor() {}
    private static final XInfoRouteProcessor xirp = new XInfoRouteProcessor()
    public static XInfoRouteProcessor getInstance() { return xirp }

    @Override
    void process(Exchange exchange) throws Exception {

    }
}
