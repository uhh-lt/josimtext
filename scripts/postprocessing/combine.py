from combine_lib import combine_majority, combine_ranks, combine_weighted_average
from interpret_lib import load_lexsamples, predict_lexsample
from os.path import join
import argparse


def combine(lexsample_fpaths, output_dir):
    lexsamples = load_lexsamples(lexsample_fpaths, header=None)
    for fpath in lexsamples:
        predict_lexsample(lexsamples[fpath], join(output_dir, fpath + "-re2.csv"))
    lexsamples = load_lexsamples([fpath + "-re2.csv" for fpath in lexsample_fpaths], header=None)

    cmb_name = "+".join(x.split("-")[0] for x in lexsamples)
    combine_majority(lexsamples, join(output_dir, "cmb-majority-%s.csv" % cmb_name ))
    combine_weighted_average(lexsamples, join(output_dir, "cmb-weighted-avg-reject50-%s.csv" % cmb_name), reject_more_than_50=True)
    combine_weighted_average(lexsamples, join(output_dir, "cmb-weighted-avg-%s.csv" % cmb_name), reject_more_than_50=False)
    combine_ranks(lexsamples, join(output_dir, "cmb-ranks-%s.csv" % cmb_name))
    combine_ranks(lexsamples, join(output_dir, "cmb-ranks-ignore-reject-%s.csv" % cmb_name), ignore_rejected=True)

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

    combine([args.clusters], args.output)
    # combine([args.clusters, args.coocs, args.depstarget, args.depsall, args.trigramstarget, args.trigramsall], args.output)
    combine([args.clusters, args.coocs], args.output)
    combine([args.coocs, args.depstarget], args.output)
    combine([args.clusters, args.coocs, args.depstarget, args.trigramstarget], args.output)
    combine([args.clusters, args.coocs, args.depsall, args.trigramsall], args.output)
    combine([args.clusters, args.coocs, args.depstarget], args.output)
    combine([args.clusters, args.coocs, args.depsall], args.output)

if __name__ == '__main__':
    main()
