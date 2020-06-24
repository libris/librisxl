package datatool

import groovy.cli.commons.CliBuilder
import datatool.scripts.mergeworks.MergeWorks

class WorkTool {
    public static void main(String[] args) {
        
        def cli = new CliBuilder(usage:'whelktool [options] <SCRIPT>')
        cli.h(longOpt: 'help', 'Print this help message and exit.')
        cli.m(longOpt:'merge', 'Merge')
        cli.s(longOpt:'show', 'Show')
        cli.d(longOpt:'diff', args: 1, argName:'diff', 'Field to diff')
        cli.e(longOpt:'edition', 'editionStatement')
        cli.t(longOpt:'subTitles', 'Subtitles')
        cli.f(longOpt:'fiction', 'Mixed marc/FictionNotFurtherSpecified and marc/NotFictionNotFurtherSpecified')
        cli.f2(longOpt:'fiction2', 'Fiction')

        def options = cli.parse(args)
        if (options.h) {
            cli.usage()
            System.exit 0
        }

        def clustersPath = options.arguments()[0]
        def m = new MergeWorks(new File(clustersPath))
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
        else if (options.t) {
            m.subTitles()
        }
        else if (options.e) {
            m.edition()
        }
        else if (options.f) {
            m.fiction()
        }
        else if (options.f2) {
            m.fiction2()
        }
        else {
            cli.usage()
            System.exit 1
        }
    }
}
