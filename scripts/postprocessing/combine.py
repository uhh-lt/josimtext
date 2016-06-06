from combine_lib import combine_majority, combine_ranks, combine_weighted_average
from interpret_lib import load_lexsamples2, predict_lexsample
from os.path import join
import argparse


def combine(lexsamples, output_dir):
    cmb_name = "+".join(lexsamples.keys())
    combine_majority(lexsamples, join(output_dir, "cmb-majority-%s.csv" % cmb_name ))
    combine_weighted_average(lexsamples, join(output_dir, "cmb-weighted-avg-reject50-%s.csv" % cmb_name), reject_more_than_50=True)
    combine_weighted_average(lexsamples, join(output_dir, "cmb-weighted-avg-%s.csv" % cmb_name), reject_more_than_50=False)
    combine_ranks(lexsamples, join(output_dir, "cmb-ranks-%s.csv" % cmb_name))
    combine_ranks(lexsamples, join(output_dir, "cmb-ranks-ignore-reject-%s.csv" % cmb_name), ignore_rejected=True)


def subset(lexsamples, targets):
    return {name: lexsamples[name] for name in lexsamples if name in targets}


def main():
    lexsample_description = 'Output of the WSD class: a lexical sample file with features and predictions (18 columns, no header)'
    parser = argparse.ArgumentParser(description='Combine single classifiers in different ways using meta-combinations.')
    parser.add_argument('output', help="Output directory with results.")
    parser.add_argument('clusters', help=lexsample_description)
    parser.add_argument('coocs', help=lexsample_description)
    parser.add_argument('depstarget', help=lexsample_description)
    parser.add_argument('depsall', help=lexsample_description)
    parser.add_argument('trigramstarget', help=lexsample_description)
    parser.add_argument('trigramsall', help=lexsample_description)
    args = parser.parse_args()
    print "Output:", args.output
    print "Clusters:", args.clusters
    print "Coocs:", args.coocs
    print "Depstarget:", args.depstarget
    print "Depsall:", args.depsall
    print "Trigramstarget:", args.trigramstarget
    print "Trigramsall:", args.trigramsall

    lexsample_fpaths = {"Clusters": args.clusters,
                        "Coocs": args.coocs,
                        "Depstarget": args.depstarget,
                        "Trigramstarget": args.trigramstarget,
                        "Depsall": args.depsall,
                        "Trigramsall": args.trigramsall}

    lexsamples_re2_fpaths = {name: join(args.output, name + "-re2.csv") for name in lexsample_fpaths}
    lexsamples = load_lexsamples2(lexsample_fpaths, header=None)
    for fname in lexsamples:
        predict_lexsample(lexsamples[fname], lexsamples_re2_fpaths[fname])
    lexsamples_re2 = load_lexsamples2(lexsamples_re2_fpaths, header=None)

    combine(subset(lexsamples_re2, ["Clusters"]), args.output)
    # combine([lexsamples_re2["Clusters"], lexsamples_re2["Coocs"], lexsamples_re2["Depstarget"], lexsamples_re2["Depsall"], lexsamples_re2["Trigramstarget"], lexsamples_re2["Trigramsall"]], args.output)
    combine(subset(lexsamples_re2, ["Clusters", "Coocs"]), args.output)
    combine(subset(lexsamples_re2, ["Coocs", "Depstarget"]), args.output)
    combine(subset(lexsamples_re2, ["Clusters","Coocs","Depstarget","Trigramstarget"]), args.output)
    combine(subset(lexsamples_re2, ["Clusters","Coocs","Depsall","Trigramsall"]), args.output)
    combine(subset(lexsamples_re2, ["Clusters","Coocs","Depstarget"]), args.output)
    combine(subset(lexsamples_re2, ["Clusters","Coocs","Depsall"]), args.output)

if __name__ == '__main__':
    main()
