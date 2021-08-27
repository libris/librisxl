package datatool

import groovy.cli.commons.CliBuilder
import datatool.scripts.mergeworks.WorkToolJob

/**
  1) find clusters
  $ ENV=local && time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --dry-run scripts/analysis/works2.groovy

  2) merge overlapping clusters, output file is placed in same directory as input
 
  $ CLUSTERSDIR=reports/local-2021...
  $ ENV=local && time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -DclustersDir=$CLUSTERSDIR -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --dry-run scripts/analysis/merge-clusters.groovy

  3) 
  ENV=local && time java -Xmx4G -Dxl.secret.properties=$HOME/secret.properties-$ENV -cp build/libs/whelktool.jar datatool.WorkTool --dry-run -s reports/1000-fiction.tsv


 */

class WorkTool {
    static void main(String[] args) {
        def cli = new CliBuilder(usage:'whelktool [options] <SCRIPT>')
        cli.h(longOpt: 'help', 'Print this help message and exit.')
        cli.I(longOpt:'skip-index', 'Do not index any changes, only write to storage.')
        cli.d(longOpt:'dry-run', 'Do not save any modifications.')
        cli.a(longOpt:'allow-loud', 'Do loud modifications.')

        cli.m(longOpt:'merge', 'Merge')
        cli.s(longOpt:'show', 'Show. Generate HTML report with title clusters')
        cli.s2(longOpt:'show2', 'Show. Generate HTML report with works')
        cli.dd(longOpt:'diff', args: 1, argName:'diff', 'Field to diff')
        cli.i(longOpt:'instance-vals', args: 1, argName:'field', 'Instance field to print, e.g. editionStatement')
        cli.t(longOpt:'subTitles', 'Print subtitles')
        cli.nf(longOpt:'fiction-not-fiction', 'Filter: output clusters with mixed marc/FictionNotFurtherSpecified and marc/NotFictionNotFurtherSpecified')
        cli.f(longOpt:'fiction', 'Filter: output clusters containing fiction')
        cli.tc(longOpt:'title-clusters', 'Filter: output title clusters')
        cli.lc(longOpt:'link-contribution', 'link matching contribution within cluster')

        def options = cli.parse(args)
        if (options.h) {
            cli.usage()
            System.exit 0
        }

        def clustersPath = options.arguments()[0]
        def m = new WorkToolJob(new File(clustersPath))
        m.skipIndex = options.I
        m.dryRun = options.d
        m.loud = options.a

        if (options.m) {
            m.merge()
        }
        else if (options.s) {
            m.show()
        }
        else if (options.s2) {
            m.show2()
        }
        else if (options.t) {
            m.subTitles()
        }
        else if (options.i) {
            m.printInstanceValue(options.i)
        }
        else if (options.nf) {
            m.fictionNotFiction()
        }
        else if (options.f) {
            m.fiction()
        }
        else if (options.tc) {
            m.outputTitleClusters()
        }
        else if (options.lc) {
            m.linkContribution()
        }
        else {
            cli.usage()
            System.exit 1
        }
    }
}
