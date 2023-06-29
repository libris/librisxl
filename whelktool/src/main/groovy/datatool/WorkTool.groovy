package datatool

import datatool.scripts.mergeworks.Doc
import groovy.cli.commons.CliBuilder
import datatool.scripts.mergeworks.WorkToolJob

/**
 1) find clusters
 $ ENV=local && time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --dry-run scripts/analysis/find-work-clusters.groovy

 2) merge overlapping clusters, output file is placed in same directory as input

 $ CLUSTERSDIR=reports/local-2021...
 $ ENV=local && time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -DclustersDir=$CLUSTERSDIR -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --dry-run scripts/analysis/merge-clusters.groovy

 3)
 ENV=local && time java -Xmx4G -Dxl.secret.properties=$HOME/secret.properties-$ENV -cp build/libs/whelktool.jar datatool.WorkTool --dry-run -s reports/1000-swedishFiction.tsv


 */

class WorkTool {
    static void main(String[] args) {
        def cli = new CliBuilder(usage: 'WorkTool [options] <CLUSTER TSV>')
        cli.h(longOpt: 'help', 'Print this help message and exit.')
        cli.I(longOpt: 'skip-index', 'Do not index any changes, only write to storage.')
        cli.d(longOpt: 'dry-run', 'Do not save any modifications.')
        cli.a(longOpt: 'allow-loud', 'Do loud modifications.')
        cli.nt(longOpt:'num-threads', args:1, argName:'N', "Override default number of threads.")
        cli.v(longOpt: 'verbose', '.')
        cli.r(longOpt: 'report', args: 1, argName: 'report dir', 'Save reports in this directory')

        cli.m(longOpt: 'merge', 'Merge and extract matching works')
        cli.s(longOpt: 'show', 'Show. Generate HTML report with title clusters')
        cli.s2(longOpt: 'showWorks', 'Show. Generate HTML report with works')
        cli.sh(longOpt: 'showHubs', 'Show. Generate HTML report with title clusters containing different works')
        cli.f(longOpt: 'swedishFiction', 'Filter: output clusters containing swedish fiction')
        cli.tr(longOpt: 'anonymousTranslation', 'Filter: remove translations without translator')
        cli.tc(longOpt: 'title-clusters', 'Filter: output title clusters')

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
        m.verbose = options.v
        m.reportDir = options.r ? new File(options.r) : m.reportDir
        m.numThreads = options.nt ? Integer.parseInt(options.nt) : -1

        if (options.m) {
            m.merge()
        } else if (options.s) {
            m.show()
        } else if (options.s2) {
            m.showWorks()
        } else if (options.sh) {
            m.showHubs()
        } else if (options.f) {
            m.swedishFiction()
        } else if (options.tr) {
            m.filterClusters({ Doc d -> !d.isAnonymousTranslation() })
        } else if (options.tc) {
            m.outputTitleClusters()
        } else {
            cli.usage()
            System.exit 1
        }
    }
}
