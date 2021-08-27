package datatool.scripts.mergeworks.compare;

class Extent implements FieldHandler {

    // TODO: allow one side missing extent (-1)?
    // TODO: improve parsing https://metadatabyran.kb.se/beskrivning/materialtyper-arbetsfloden/tryckta-monografier/omfang-for-tryckta-monografier
    @Override
    boolean isCompatible(Object a, Object b) {
        return a * 0.7 < b && a * 1.3 > b
    }

    @Override
    Object merge(Object a, Object b) {
        return b; // not part of final work
    }
}
