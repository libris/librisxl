package whelk.component

import whelk.Document
import whelk.util.CamelSender

/**
 * Created by markus on 2015-11-17.
 */
class ApixClientCamel extends CamelSender implements APIX {

    ApixClientCamel(String apixActiveMQConnection, String apixQueueName) {
        super(apixActiveMQConnection, apixQueueName)
    }

    @Override
    void send(Document doc) {
        sendObjectMessage(doc.data)
    }
}
