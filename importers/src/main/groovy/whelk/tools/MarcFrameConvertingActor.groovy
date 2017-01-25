package whelk.tools

import groovy.util.logging.Slf4j
import groovyx.gpars.actor.DefaultActor
import whelk.converter.marc.MarcFrameConverter

/**
 * Created by Theodor on 2017-01-21.
 * Actor that hosts a MarcFrameConverter for future (and past) experiments with parallelism
 */
@Slf4j
class MarcFrameConvertingActor extends DefaultActor {
    MarcFrameConverter marcFrameConverter

    MarcFrameConvertingActor() {
        marcFrameConverter = new MarcFrameConverter()
    }

    @Override
    protected void act() {
        loop {
            react { argument ->
                try {
                    def doc = argument.spec == null ? marcFrameConverter.convert(argument.doc as Map, argument.id as String) :
                            marcFrameConverter.convert(argument.doc as Map, argument.id as String, argument.spec as Map)
                    if (!doc) {
                        throw new Exception()
                    }
                    reply doc
                }
                catch (any) {
                    println any.message
                    println argument.inspect()
                    reply null
                }
            }
        }
    }
}
