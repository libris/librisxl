package whelk.tools

import groovyx.gpars.actor.DefaultActor

/**
 * Created by theodortolstoy on 2017-01-11.
 * Actor from printing to stdout. Perhaps unnecessary.
 */
class StatsPrinter extends DefaultActor {
    def counter = 0
    def startTime = System.currentTimeMillis()

    @Override
    protected void act() {
        loop {
            react { argument ->

                if (++counter % 10000 == 0) {
                    def elapsedSecs = (System.currentTimeMillis() - startTime) / 1000
                    if (elapsedSecs > 0) {
                        def docsPerSec = counter / elapsedSecs
                        println "Working. Currently ${counter} documents sent. Crunching ${docsPerSec} docs / s."

                    }
                }

            }
        }
    }
}
