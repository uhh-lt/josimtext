import argparse
from interpret_lib import load_lexsamples, predict_lexsample, plot_features
import os
from os.path import join

INTERPRET_ONLY_WRONG = True


def main():
    parser = argparse.ArgumentParser(description='Interpret sense predictions of the JoSimText system.')
    parser.add_argument('lexsample', help='Output of the WSD class: a lexical sample file with features and '
                                          'predictions (18 columns, no header)')
    args = parser.parse_args()
    print "Lexical sample file:", args.lexsample

    lexsample_dir = os.path.dirname(args.lexsample)
    lexsamples = load_lexsamples([args.lexsample], header=None)
    re_fpaths = []
    for fpath in lexsamples:
        re_fpath = join(lexsample_dir, fpath + "-re.csv")
        predict_lexsample(lexsamples[fpath], re_fpath)
        re_fpaths.append(re_fpath)

    lexsamples = load_lexsamples(re_fpaths, header=None)
    plot_features(lexsamples, context_ids=[])


if __name__ == '__main__':
    main()