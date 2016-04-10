from ast import literal_eval
from collections import Counter
from pandas import Series
import numpy as np


LIST_SEP = "  "
TMP_SEP = "\t"

def get_best_id(predict_sense_ids):
    """ Converts a string '1:0.9, 2:0.1' to '1', or just keeps the simple format the same e.g. '1' -> '1'. """

    try:
        ids = predict_sense_ids.split(LIST_SEP)
        scores = Counter()
        for s in ids:
            p = s.split(":")
            label = p[0]
            conf = float(p[1]) if len(p) == 2 else 1.0
            scores[label] = conf
        major_label = scores.most_common(1)[0][0]

        return major_label
    except:
        print predict_sense_ids
        print format_exc()
        return "-1"


def combine_majority(lexsamples, output_fpath):
    """ Performs combination via majority voting. Uses  """

    if len(lexsamples) == 0: return None

    keys = lexsamples.keys()
    predict_senses = lexsamples[keys[0]].predict_sense_ids.copy()
    for k in keys[1:]:
        predict_senses += TMP_SEP + lexsamples[k].predict_sense_ids

    lexsample_majority = lexsamples[lexsamples.keys()[0]].copy()
    result = []
    for i, ids_str in enumerate(predict_senses):
        ids = [get_best_id(ids_model) for ids_model in ids_str.split(TMP_SEP)]
        scores = Counter(ids)
        result.append(LIST_SEP.join([u"%s:%.2f" % (label, scores[label]/float(len(ids))) for label in scores]))

    lexsample_majority.predict_sense_ids=Series(np.array(result), index=lexsample_majority.index)
    lexsample_majority.to_csv(output_fpath, sep="\t", encoding="utf-8", float_format='%.0f', index=False, header=None)
    print output_fpath

    return lexsample_majority


def combine_weighted_average(lexsamples, output_fpath, reject_more_than_50=False):
    """ Performs combination via weighted average. """

    if len(lexsamples) == 0: return None

    keys = lexsamples.keys()
    predict_senses = lexsamples[keys[0]].predict_sense_ids.copy()
    for k in keys[1:]:
        predict_senses += "\t" + lexsamples[k].predict_sense_ids

    lexsample_majority = lexsamples[lexsamples.keys()[0]].copy()
    result = []
    for i, ids_str in enumerate(predict_senses):
        ids = [ids_model for ids_model in ids_str.split("\t")]
        scores = Counter()

        # reject?
        reject_count = 0.0
        ids_noreject = []
        for confs in ids:
            if confs == "-1": reject_count += 1
            else: ids_noreject.append(confs)
        if reject_more_than_50 and reject_count / len(ids) > 0.5:
            result.append("-1")
            continue

        for confs_dict in [literal_eval("{" + x.replace(LIST_SEP, ",") + "}") for x in ids_noreject]:
            scores.update(confs_dict)

        comb_confs = LIST_SEP.join([u"%s:%.2f" % (label, scores[label]) for label in scores])
        result.append(comb_confs)

    lexsample_majority.predict_sense_ids=Series(np.array(result), index=lexsample_majority.index)
    lexsample_majority.to_csv(output_fpath, sep="\t", encoding="utf-8", float_format='%.0f', index=False, header=None)
    print output_fpath

    return lexsample_majority


def confs2ranks(confs):
    """ Converts a confs string e.g. '-1' or '0:-199,1:-99' to a dictionary with scores. """

    scores = {}
    for x in confs.split(LIST_SEP):
        f = x.split(":")
        if len(f) == 2:
            scores[f[0]] = float(f[1])
        else:
            scores[f[0]] = 1

    ranks = {}
    for i, x in enumerate(sorted(scores, key=scores.get, reverse=True)):
        ranks[x] = i + 1

    return ranks


def combine_ranks(lexsamples, output_fpath, ignore_rejected=False):
    """ Performs combination via rank sum. """

    if len(lexsamples) == 0: return None

    keys = lexsamples.keys()
    predict_senses = lexsamples[keys[0]].predict_sense_ids.copy()
    for k in keys[1:]:
        predict_senses += "\t" + lexsamples[k].predict_sense_ids

    lexsample_majority = lexsamples[lexsamples.keys()[0]].copy()
    result = []
    for i, ids_str in enumerate(predict_senses):
        ids = [confs for confs in ids_str.split("\t") if not ignore_rejected or confs != "-1"]
        scores = Counter()

        for confs_dict in [confs2ranks(x) for x in ids]:
            scores.update(confs_dict)

        comb_confs = LIST_SEP.join([u"%s:%.2f" % (label, 1./scores[label]) for label in scores])
        result.append(comb_confs)

    lexsample_majority.predict_sense_ids=Series(np.array(result), index=lexsample_majority.index)
    lexsample_majority.to_csv(output_fpath, sep="\t", encoding="utf-8", float_format='%.0f', index=False, header=None)
    print output_fpath

    return lexsample_majority
