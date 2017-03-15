package whelk.actors

import groovyx.gpars.actor.DefaultActor
import groovy.util.logging.Slf4j as Log
/**
 * Created by theodortolstoy on 2017-01-11.
 * Actor from printing to stdout. Perhaps unnecessary.
 */
@Log
class StatsPrinter extends DefaultActor {
    def rows = 0
    def records = 0
    def suppressed = 0
    def startTime = System.currentTimeMillis()

    @Override
    protected void act() {
        loop {
            react { argument ->
                switch (argument?.type) {
                    case 'report':
                        printReport()
                        break
                    case 'row':
                        rows++
                        break
                    case 'suppressed':
                        suppressed++
                        break
                    case 'record':
                        if (++records % 10000 == 0)
                            printReport()
                        break
                    default:
                        break
                }
                reply true
            }
        }
    }

    void printReport() {
        def elapsedSecs = (System.currentTimeMillis() - startTime) / 1000
        if (elapsedSecs > 0) {
            def docsPerSec = records > 0 ? records / elapsedSecs : 0
            def message = "Working. Currently ${rows} rows recieved and ${records} records sent. Crunching ${docsPerSec} records / s."
            println message
            log.info message


        }

    }

}
