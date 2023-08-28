package whelk.converter.marc

class NormalizeContentTypeStep extends MarcFramePostProcStepBase {
    def contentType = 'contentType'
    def shape = [TYPE, contentType] as Set

    void modify(Map record, Map thing) {
        moveContentType(thing)
    }

    boolean moveContentType(Map thing) {
        def work = thing.instanceOf
        def moved = work?.hasPart?.removeAll { p ->
            if (p.keySet() == shape) {
                work[contentType] = (ld.asList(work[contentType]) + p[contentType]).unique()
                return true
            }
        }
        if (work?.hasPart?.isEmpty()) {
            work.remove('hasPart')
        }
        return moved
    }

    void unmodify(Map record, Map thing) {
    }
}
