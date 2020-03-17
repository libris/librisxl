package datatool

import groovy.cli.commons.CliBuilder
import datatool.scripts.mergeworks.WorkJob

/**

  ENV=qa && time java -Xmx4G -Dxl.secret.properties=$HOME/secret.properties-$ENV -cp build/libs/whelktool.jar datatool.WorkTool -s reports/1000-fiction.tsv


 */

class WorkTool {
    public static void main(String[] args) {
        def cli = new CliBuilder(usage:'whelktool [options] <SCRIPT>')
        cli.h(longOpt: 'help', 'Print this help message and exit.')
        cli.I(longOpt:'skip-index', 'Do not index any changes, only write to storage.')
        cli.d(longOpt:'dry-run', 'Do not save any modifications.')
        cli.a(longOpt:'allow-loud', 'Do loud modifications.')

        cli.m(longOpt:'merge', 'Merge')
        cli.s(longOpt:'show', 'Show. Generate HTML report with title clusters')
        cli.s2(longOpt:'show', 'Show. Generate HTML report with works')
        cli.d(longOpt:'diff', args: 1, argName:'diff', 'Field to diff')
        cli.e(longOpt:'edition', 'Print editionStatement')
        cli.t(longOpt:'subTitles', 'Print subtitles')
        cli.nf(longOpt:'fiction-not-fiction', 'Filter: output clusters with mixed marc/FictionNotFurtherSpecified and marc/NotFictionNotFurtherSpecified')
        cli.f(longOpt:'fiction', 'Filter: output clusters containing fiction')

        def options = cli.parse(args)
        if (options.h) {
            cli.usage()
            System.exit 0
        }

        def clustersPath = options.arguments()[0]
        def m = new WorkJob(new File(clustersPath))
        m.skipIndex = options.I
        m.dryRun = options.d
        m.loud = options.a

        if (options.m) {
            m.merge()
        }
        else if (options.s) {
            if (options.d) {
                m.show(options.d.split('\\,').collect{it.split('\\.')})
            }
            else {
                m.show()
            }
        }
        else if (options.s2) {
            m.show2()
        }
        else if (options.t) {
            m.subTitles()
        }
        else if (options.e) {
            m.edition()
        }
        else if (options.nf) {
            m.fictionNotFiction()
        }
        else if (options.f) {
            m.fiction()
        }
        else {
            cli.usage()
            System.exit 1
        }
    }
}
