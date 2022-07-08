package datatool.scripts.mergeworks.compare

class Subject extends StuffSet {
    @Override
    Object merge(Object a, Object b) {
        return super.merge(a, b).findAll { it.'@id' || it.'@type' == 'ComplexSubject' }
    }
}
