from pandas import read_csv
from ntpath import basename
from os.path import join
from traceback import format_exc
from collections import defaultdict
import math
from collections import Counter
import numpy as np
from pandas import Series

from combine_lib import LIST_SEP
DEF_SCORE = 0.000010
SEP = "  "
VERBOSE = False


names=["context_id","target","target_pos","target_position","gold_sense_ids","predict_sense_ids","golden_related","predict_related","context",
       "word_features","holing_features","target_holing_features",
       "conf","used_features_num","best_conf_norm","sense_priors", "used_features","all_features"]


def load_lexsample(fpath, header):

    df = read_csv(fpath, encoding='utf-8', delimiter="\t",
                  error_bad_lines=True, escapechar=u"\0", quotechar=u"\0", doublequote=False,
                  header=header, names=names, low_memory=True)
    df.gold_sense_ids = df.gold_sense_ids.astype(unicode)
    df.predict_sense_ids = df.predict_sense_ids.astype(unicode)
    df = df.fillna("")
    return df


def load_lexsamples(lexsample_fpaths, header):
    """ Load a list of lexsamples 'as is' into a dictionary fname->dataframe. """

    lexsamples = {basename(fpath): load_lexsample(fpath, header) for fpath in lexsample_fpaths}

    lens = {l:len(lexsamples[l]) for l in lexsamples}
    lens_equal = True
    first = lens.values()[0]
    for l in lens.values():
        if l != first:
            lens_equal = False
            break

    if not lens_equal:
        print "Warning: lengths of the data are not equal:"
        print lens

    return lexsamples


def predict_context(used_dict):
    """ Predict class of the system based on a dictionary of used features for classification.
    The result is a sting with confedences e.g. '0:-199.0,1:-99'. If no features, or only features
    with default values, then reject classification. """

    only_zeros = True
    feature_scores = defaultdict(dict)

    # reformat
    for label in used_dict:
        for feature in used_dict[label]:
            if feature in used_dict[label] and float(used_dict[label][feature]) > DEF_SCORE:
                feature_scores[feature][label] = float(used_dict[label][feature])
                only_zeros = False
            else:
                feature_scores[feature][label] = DEF_SCORE

    # calculate probs
    label_probs = defaultdict(float)
    for feature in feature_scores:
        for label in feature_scores[feature]:
            label_probs[label] += math.log(feature_scores[feature][label])

    label_probs = [(label, label_probs[label]) for label in sorted(label_probs, key=label_probs.get, reverse=True)]

    if len(label_probs) == 0 or only_zeros:
        return "-1"
    else:
        return LIST_SEP.join(["%s:%.2f" % (label, score) for label, score in label_probs])


def used_all_to_string(used_all):
    res = []
    for label in used_all:
        for feature in sorted(used_all[label], key=used_all[label].get, reverse=True):
            res.append("%s:%s:%s" % (label, feature, used_all[label][feature]))

    return "  ".join(res)


def used_features_with_scores(used_features, all_features):
    """ From a list of used features and a dictionary of all features construct
    a dictionary of used features. """

    used_set = set(used_features.split(SEP))

    all_dict = defaultdict(dict)
    for x in all_features.split(SEP):
        try:
            label, feature, score = x.split(":")
            score = float(score) if float(score) > DEF_SCORE else DEF_SCORE
            all_dict[label][feature] = score
        except:
            if VERBOSE: print "Error:", x


    used_dict = defaultdict(dict)
    for label in all_dict:
        for feature in all_dict[label]:
            if feature in used_set:
                used_dict[label][feature] = all_dict[label][feature]

    return used_dict


def predict_lexsample(lexsample, output_fpath):
    """ Re-predicts labels from the lexical sample. Changes fields 'predict_sense_ids' and 'used_features'. """

    predict_ids = []
    used_features = []
    for i, row in lexsample.iterrows():
        used_all = used_features_with_scores(row.used_features, row.all_features)
        predict_ids.append(predict_context(used_all))
        used_features.append(used_all_to_string(used_all))
        if i % 20000 == 0: print i

    print "# skipped contexts:", len([x for x in predict_ids if x == "" or x == "-1"])
    lexsample_re = lexsample.copy()
    lexsample_re.predict_sense_ids = Series(np.array(predict_ids), index=lexsample_re.index)
    lexsample_re.used_features = Series(np.array(used_features), index=lexsample_re.index)
    lexsample_re.to_csv(output_fpath, sep="\t", encoding="utf-8", float_format='%.0f', index=False, header=False)
    print "Re-predicted lexsample:", output_fpath



def parse_used_features(all_features):
    all_dict = defaultdict(dict)

    for x in all_features.split(SEP):
        try:
            label, feature, score = x.split(":")
            score = float(score) if float(score) > DEF_SCORE else DEF_SCORE
            all_dict[label][feature] = score
        except:
            if VERBOSE: print "Error:", x

    return all_dict


def sort_by_relevance(used_dict):
    predict_context(used_dict)

    feature_scores = defaultdict(list)
    for label in used_dict:
        for feature in used_dict[label]:
            feature_scores[feature].append( (float(used_dict[label][feature]), label) )

    feature_scores = {feature: sorted(feature_scores[feature], reverse=True) for feature in feature_scores}
    feature_relevance = {feature: (feature_scores[feature][0][0]/feature_scores[feature][-1][0], feature_scores[feature][0][1]) for feature in feature_scores}
    feature_relevance_s = [(feature, feature_relevance[feature]) for feature in sorted(feature_relevance, key=feature_relevance.get, reverse=True)]

    return feature_relevance_s


def plot_context(row):
    print row.context_id, row.target, "\n"
    predict_sense_id = get_best_id(row.predict_sense_ids)
    status = "OK" if row.gold_sense_ids == predict_sense_id else "ERR"
    print "%s\tgold/predict: %s/%s (%s)" % (status, row.gold_sense_ids, predict_sense_id, row.predict_sense_ids), "\n"
    print "gold (%s): %s" % (row.gold_sense_ids, row.golden_related[:200]), "\n"
    predict_id = get_best_id(row.predict_sense_ids)
    print "predict (%s): %s" % (predict_id, row.predict_related[:200] if predict_id != "-1" else ""), "\n"
    print "context:", row.context[:200], "\n"
    print "used features:", row.used_features[:200], "\n"
    print "all features:", row.all_features[:200], "\n"

    features_by_relevance = []
    used_dict = parse_used_features(row.used_features)
    for feature, score_label in sort_by_relevance(used_dict):
        features_by_relevance.append("%s:%.2f:%s" % (feature, score_label[0], score_label[1]))
    print "sorted by relevance: %s" % "  ".join(features_by_relevance)


def plot_features(lexsamples, context_ids=[]):

    for k in lexsamples.keys():
        print "\n\n", "="*  50
        print k, "\n"

        for i, row in lexsamples[k].iterrows():
            print "\n\n", i, "-"*  50, "\n"

            plot_context(row)


##############################################
# can be imported from twsi_evaluation import get_best_id

def get_best_id(predict_sense_ids, sep=LIST_SEP):
    """ Converts a string '1:0.9, 2:0.1' to '1', or just keeps the simple format the same e.g. '1' -> '1'. """

    try:
        ids = predict_sense_ids.split(sep)
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
######################################
