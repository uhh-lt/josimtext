from combine_lib import combine_majority, combine_ranks, combine_weighted_average
from interpret_lib import load_lexsamples, predict_lexsample
from os.path import join


def combine(lexsample_fpaths):
    lexsamples = load_lexsamples(lexsample_fpaths, header=None)
    for fpath in lexsamples:
        predict_lexsample(lexsamples[fpath], join(output_dir, fpath + "-re2.csv"))
    lexsamples = load_lexsamples([fpath + "-re2.csv" for fpath in lexsample_fpaths], header=None)

    cmb_name = "+".join(x.split("-")[0] for x in lexsamples)
    lexsample_cmb = combine_majority(lexsamples, join(output_dir, "cmb-majority-%s.csv" % cmb_name ))
    lexsample_cmb = combine_weighted_average(lexsamples, join(output_dir, "cmb-weighted-avg-reject50-%s.csv" % cmb_name), reject_more_than_50=True)
    lexsample_cmb = combine_weighted_average(lexsamples, join(output_dir, "cmb-weighted-avg-%s.csv" % cmb_name), reject_more_than_50=False)
    lexsample_cmb = combine_ranks(lexsamples, join(output_dir, "cmb-ranks-%s.csv" % cmb_name))
    lexsample_cmb = combine_ranks(lexsamples, join(output_dir, "cmb-ranks-ignore-reject-%s.csv" % cmb_name), ignore_rejected=True)



clusters_fpath = "/Users/alex/Desktop/prj/test-res/results/Clusters-3"
coocs_fpath = "/Users/alex/Desktop/prj/test-res/results/Depwords-4"
depstarget_fpath = "/Users/alex/Desktop/prj/test-res/results/Depstarget-2"
depsall_fpath = "/Users/alex/Desktop/prj/test-res/results/Depsall-4"
trigramstarget_fpath = "/Users/alex/Desktop/prj/test-res/results/Trigramstarget-3"
trigramsall_fpath = "/Users/alex/Desktop/prj/test-res/results/Trigramsall-4"

output_dir = "/Users/alex/Desktop/prj/test-res/results/"

# combine([clusters_fpath])
# combine([clusters_fpath, coocs_fpath, depstarget_fpath, depsall_fpath, trigramstarget_fpath, trigramsall_fpath])
# combine([clusters_fpath, coocs_fpath])
# combine([coocs_fpath, depstarget_fpath])
combine([clusters_fpath, coocs_fpath, depstarget_fpath, trigramstarget_fpath])
combine([clusters_fpath, coocs_fpath, depsall_fpath, trigramsall_fpath])
combine([clusters_fpath, coocs_fpath, depstarget_fpath])
combine([clusters_fpath, coocs_fpath, depsall_fpath])
