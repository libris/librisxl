package whelk.util

import spock.lang.Specification

class StatisticsSpec extends Specification {

    def "increment counts occurrences per category and name"() {
        given:
        def stats = new Statistics()

        when:
        stats.increment('category1', 'a')
        stats.increment('category1', 'a')
        stats.increment('category1', 'b')
        stats.increment('category2', 'a')

        then:
        stats.c['category1']['a'].intValue() == 2
        stats.c['category1']['b'].intValue() == 1
        stats.c['category2']['a'].intValue() == 1
    }

    def "increment throws on null category or name"() {
        given:
        def stats = new Statistics()

        when:
        stats.increment(null, 'a')

        then:
        thrown(NullPointerException)

        when:
        stats.increment('category', null)

        then:
        thrown(NullPointerException)
    }

    def "number of stored examples is capped at numExamples"() {
        given:
        def stats = new Statistics(2)

        when:
        stats.increment('category', 'name', 'example1')
        stats.increment('category', 'name', 'example2')
        stats.increment('category', 'name', 'example3')

        then:
        stats.c['category']['name'].intValue() == 3
        stats.examples['category']['name'].collect() == ['example1', 'example2']
    }

    def "no examples are stored when numExamples is zero"() {
        given:
        def stats = new Statistics(0)

        when:
        stats.increment('category', 'name', 'example')

        then:
        stats.c['category']['name'].intValue() == 1
        stats.examples.isEmpty()
    }

    def "example is taken from context if not given"() {
        given:
        def stats = new Statistics()

        when:
        stats.withContext('context example') {
            stats.increment('category', 'name')
        }

        then:
        stats.examples['category']['name'].collect() == ['context example']
    }

    def "explicit example takes precedence over context"() {
        given:
        def stats = new Statistics()

        when:
        stats.withContext('context example') {
            stats.increment('category', 'name', 'explicit example')
        }

        then:
        stats.examples['category']['name'].collect() == ['explicit example']
    }

    def "contexts nest"() {
        given:
        def stats = new Statistics()

        when:
        stats.withContext('outer') {
            stats.withContext('inner') {
                stats.increment('category', 'a')
            }
            stats.increment('category', 'b')
        }

        then:
        stats.examples['category']['a'].collect() == ['inner']
        stats.examples['category']['b'].collect() == ['outer']
    }

    def "context is popped when closure throws"() {
        given:
        def stats = new Statistics()

        when:
        stats.withContext('example') {
            throw new RuntimeException('oops')
        }

        then:
        thrown(RuntimeException)
        stats.contextExample() == null
    }

    def "contextExample is null outside context"() {
        given:
        def stats = new Statistics()

        expect:
        stats.contextExample() == null
    }

    def "isEmpty"() {
        given:
        def stats = new Statistics()

        expect:
        stats.isEmpty()

        when:
        stats.increment('category', 'name')

        then:
        !stats.isEmpty()
    }

    def "print outputs counts and examples"() {
        given:
        def stats = new Statistics()
        stats.increment('category', 'common', 'example')
        stats.increment('category', 'common')
        stats.increment('category', 'rare')
        def bytes = new ByteArrayOutputStream()

        when:
        stats.print(0, new PrintStream(bytes))
        def output = bytes.toString()

        then:
        output.contains('category (3)')
        output.contains('common')
        output.contains('rare')
        output.contains('[example]')
    }

    def "print omits entries with count <= min"() {
        given:
        def stats = new Statistics()
        stats.increment('category', 'common')
        stats.increment('category', 'common')
        stats.increment('category', 'rare')
        def bytes = new ByteArrayOutputStream()

        when:
        stats.print(1, new PrintStream(bytes))
        def output = bytes.toString()

        then:
        output.contains('common')
        !output.contains('rare')
    }
}
