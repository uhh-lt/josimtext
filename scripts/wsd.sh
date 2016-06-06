mode=DepsallCoocsClusters; time spark-submit --class WSD --master=yarn-cluster --queue=longrunning --num-executors 200 --driver-memory 8g --executor-memory 8g ~/josimtext_2.10-0.2.jar ukwac/senses/senses-ukwac-dep-cw-e0-N200-n200-minsize5-semeval-twsi-js-format.csv ukwac/features/words-from-deps-50nn-20000.csv ukwac/features/deps-10nn-20000.csv ukwac/Dataset-SemEval-2013-13-js-features.csv ukwac/semeval/output-$mode 0.00001 $mode true 20000 50 1> semeval-$mode.log 2> semeval-$mode.err 


for mode in DepsallCoocsClusters DepstargetCoocsClusters Clusters Coocs Depsall Depstarget ; do time spark-submit --class WSD --master=yarn-cluster --queue=longrunning --num-executors 200 --driver-memory 8g --executor-memory 8g ~/JoSimText/bin/spark/josimtext_2.10-0.2.jar ukwac/senses/senses-ukwac-dep-cw-e0-N200-n200-minsize5-semeval-twsi-js-format.csv ukwac/features/words-from-deps-20nn-20k ukwac/features/deps-20nn-20k ukwac/Dataset-SemEval-2013-13-js-features.csv ukwac/semeval/$mode 0.00001 $mode true 20000 50 1> semeval-$mode.log 2> semeval-$mode.err ; done

spark="/home/panchenko/spark/spark-1.6.1-bin-hadoop2.6/bin/spark-submit"
jst="/home/panchenko/spark/josimtext_2.10-0.3.jar"
m=152g
s="/home/panchenko/spark/features/ukwac/clusters-ukwac-dep-cw-e0-N200-n200-minsize5-semeval-twsi-js-format-1038-nolower-1930.csv";
c="/home/panchenko/spark/features/ukwac/depwords-50nn-20000-4cols.csv";
d="/home/panchenko/spark/features/ukwac/deps-50nn-20000-4cols.csv";
t="/home/panchenko/spark/features/ukwac/trigrams-50nn-20k-wc-4cols.csv";
l="/home/panchenko/spark/Dataset-SemEval-2013-13-js-features-new-trigrams.csv";

for mode in DepstargetCoocsClustersTrigramstarget DepsallCoocsClustersTrigramsall DepstargetCoocsClusters DepsallCoocsClusters Depstarget Depsall Coocs Clusters Trigramsall Trigramstarget ; do
    o="/home/panchenko/spark/semeval/ukwac/starsem-$mode";
    time $spark --class WSD --num-executors 16 --driver-memory $m --executor-memory $m $jst $s $c $d $t $l $o $mode true 200000 16  
done

exit




spark="/home/panchenko/spark/spark-1.6.1-bin-hadoop2.6/bin/spark-submit"
jst="/home/panchenko/spark/josimtext_2.10-0.3.jar"
m=154g
s="/home/panchenko/spark/senses/clusters-wiki-deps-jst-wpf1k-fpw1k-thr-cw-e0-N200-n200-minsize15-js-format-1042-lower-1692.csv";
c="/home/panchenko/spark/features/wiki/depwords-50nn-20k-word-freq-wikisenses-4cols.csv";
d="/home/panchenko/spark/features/wiki/deps-50nn-20k-word-freq-wikisenses-4cols.csv";
t="/home/panchenko/spark/features/wiki/trigrams-50nn-20k-word-freq-wikisenses-4cols.csv";
l="/home/panchenko/spark/Dataset-TWSI-2-with-js-features-new-trigrams.csv";

for mode in DepstargetCoocsClusters Depstarget Depsall Coocs Clusters Trigramsall Trigramstarget DepsallCoocsClusters DepstargetCoocsClusters DepstargetCoocsClustersTrigramstarget DepsallCoocsClustersTrigramsall; do
    o="/home/panchenko/spark/twsi/wiki/starsem-$mode";
    time $spark --class WSD --num-executors 16 --driver-memory $m --executor-memory $m $jst $s $c $d $t $l $o $mode true 200000 16  
done

exit


spark="/home/panchenko/spark/spark-1.6.1-bin-hadoop2.6/bin/spark-submit"
jst="/home/panchenko/spark/josimtext_2.10-0.3.jar"
m=132g
s="/home/panchenko/spark/senses/clusters-ukwac-dep-cw-e0-N200-n200-minsize5-semeval-twsi-js-format-1038-nolower-1930.csv";
c="/home/panchenko/spark/features/ukwac/depwords-50nn-20000-4cols.csv";
d="/home/panchenko/spark/features/ukwac/deps-50nn-20000-4cols.csv";
t="/home/panchenko/spark/features/ukwac/trigrams-50nn-20k-wc-4cols.csv";
l="/home/panchenko/spark/Dataset-TWSI-2-with-js-features-new-trigrams.csv";

for mode in DepstargetCoocsClusters Depstarget Depsall Coocs Clusters Trigramsall Trigramstarget DepsallCoocsClusters DepstargetCoocsClusters DepstargetCoocsClustersTrigramstarget DepsallCoocsClustersTrigramsall; do
    o="/home/panchenko/spark/twsi/ukwac/starsem-$mode";
    time $spark --class WSD --num-executors 16 --driver-memory $m --executor-memory $m $jst $s $c $d $t $l $o $mode true 200000 16  
done

exit


for norm in word-freq ; do for coocs in words-from-deps ; do for nn in 50 ; do for mode in Depstarget ; do time spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class WSD --num-executors 16 --driver-memory 64g --executor-memory 64g josimtext_2.10-0.2.jar senses/senses-wiki-deps-jst-wpf1k-fpw1k-thr-cw-e0-N200-n200-minsize15-js-format-1042-lower-1692.csv features/wiki/${coocs}-${nn}nn-20k-${norm}-wikisenses.csv features/wiki/deps-${nn}nn-20k-${norm}-wikisenses.csv Dataset-TWSI-2-js-features.csv twsi/wiki/${mode}-${norm}-${coocs}-${nn}nn-conf.csv $mode true 200000 16 2> twsi/log.err ; done ; done ; done ; done

for norm in word-freq ; do for coocs in words-from-deps ; do for nn in 50 ; do for mode in DepstargetCoocsClusters ; do time spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class WSD --num-executors 16 --driver-memory 100g --executor-memory 100g josimtext_2.10-0.2.jar senses/senses-wiki-deps-jst-wpf1k-fpw1k-thr-cw-e0-N200-n200-minsize15-js-format-1042-lower-1692.csv features-wiki/${coocs}-${nn}nn-20k-${norm}-wikisenses.csv features-wiki/deps-${nn}nn-20k-${norm}-wikisenses.csv Dataset-TWSI twsi/wiki-nofeatures 0.00001 $mode true 0 16  2> twsi/nofeatures.err ; done ; done ; done ; done

for norm in word-freq ; do for coocs in words-from-deps ; do for nn in 50 ; do for mode in  DepstargetCoocsClusters ; do time spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class WSD --num-executors 16 --driver-memory 100g --executor-memory 100g josimtext_2.10-0.2.jar senses/senses-wiki-deps-jst-wpf1k-fpw1k-thr-cw-e0-N200-n200-minsize15-js-format-1042-lower-1692.csv features-wiki/${coocs}-${nn}nn-20k-${norm}-wikisenses.csv features-wiki/deps-${nn}nn-20k-${norm}-wikisenses.csv Dataset-TW^SI semeval/wiki-$norm-$coocs-${nn}nn-$mode 0.00001 $mode true 0 16  2> twsi/wiki-$norm-$coocs-${nn}nn-$mode.err ; done ; done ; done ; done

for norm in word-freq lmi ; do for coocs in words words-from-deps ; do for nn in 10 20 50 ; do for mode in DepsallCoocsClusters DepstargetCoocsClusters Clusters Coocs Depsall Depstarget ; do time spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class WSD --num-executors 16 --driver-memory 100g --executor-memory 100g josimtext_2.10-0.2.jar senses/senses-wiki-deps-jst-wpf1k-fpw1k-thr-cw-e0-N200-n200-minsize15-js-format-1042-lower-1692.csv features-wiki/${coocs}-${nn}nn-20k-${norm}-wikisenses.csv features-wiki/deps-${nn}nn-20k-${norm}-wikisenses.csv Dataset-TWSI-2-js-features.csv twsi/wiki-$norm-$coocs-${nn}nn-$mode 0.00001 $mode true 20000 16  2> twsi/wiki-$norm-$coocs-${nn}nn-$mode.err ; done ; done ; done ; done

for norm in wc lmi ; do for nn in 10 20 50 100 ; do for mode in Depsall Depstarget ; do time spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class WSD --num-executors 16 --driver-memory 100g --executor-memory 100g josimtext-trigrams.jar na na features/trigrams-${nn}nn-20k-${norm}.csv Dataset-TWSI-2-js-features.csv twsi/trigrams-$mode-${nn}nn-${norm} 0.00001 $mode true 20000 16 2> twsi/trigrams-$mode-${nn}nn-${norm}.err ; done ; done ; done

for nn in 10 20 50 100 ; do for mode in DepsallCoocsClusters DepstargetCoocsClusters Clusters Coocs Depsall Depstarget ; do time spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class WSD --num-executors 16 --driver-memory 100g --executor-memory 100g josimtext_2.10-0.2.jar senses-ukwac-dep-cw-e0-N200-n200-minsize5-semeval-twsi-js-format.csv features/ukwac-coocs-${nn}nn-20k.csv features/deps-${nn}nn-20000.csv Dataset-TWSI-2-js-features.csv twsi/coocs-$mode-${nn}nn 0.00001 $mode true 20000 16 1> twsi/coocs-$mode-${nn}nn.log 2> twsi/coocs-$mode-${nn}nn.err ; done ; done

time spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class WSD --num-executors 16 --driver-memory 90g --executor-memory 90g josimtext_2.10-0.2.jar senses-ukwac-dep-cw-e0-N200-n200-minsize5-semeval-twsi-js-format.csv features/words-from-deps-50nn-20000.csv features/deps-10nn-20000.csv Dataset-SemEval-2013-13-js-features.csv semeval/test 0.00001 DepsallCoocsClusters true 20000 16

for mode in DepsallCoocsClusters DepstargetCoocsClusters Clusters Coocs Depsall Depstarget ; do time spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class WSD --num-executors 16 --driver-memory 132g --executor-memory 132g josimtext_2.10-0.2.jar senses-ukwac-dep-cw-e0-N200-n200-minsize5-js-format-semeval.csv features/words-from-deps-50nn-20000.csv features/deps-10nn-20000.csv Dataset-TWSI-2-js-features.csv twsi/$mode 0.00001 $mode true 20000 16 1> twsi/$mode.log 2> twsi/$mode.err ; done

for mode in DepsallCoocsClusters DepstargetCoocsClusters Clusters Coocs Depsall Depstarget ; do time spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class WSD --num-executors 16 --driver-memory 132g --executor-memory 132g josimtext_2.10-0.2.jar senses-ukwac-dep-cw-e0-N200-n200-minsize5-js-format-semeval.csv features/words-from-deps-50nn-20000.csv features/deps-10nn-20000.csv Dataset-TWSI-2-js-features.csv twsi-$mode 0.00001 $mode true 20000 16 1> $model.log 2> $mode.err ; done

for mode in DepsallCoocsClusters DepstargetCoocsClusters Clusters Coocs Depsall Depstarget ; do  spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class WSD --num-executors 16 --driver-memory 132g --executor-memory 132g josimtext_2.10-0.2.jar senses-ukwac-dep-cw-e0-N200-n200-minsize5-js-format-semeval.csv features/words-from-deps-50nn-20000.csv features/deps-10nn-20000.csv Dataset-SemEval-2013-13-js-features.csv semeval-$mode-2 0.00001 $mode true 20000 16 1> $model.log 2> $mode.err ; done ; for mode in DepsallCoocsClusters DepstargetCoocsClusters Clusters Coocs Depsall Depstarget ; do  spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class WSD --num-executors 16 --driver-memory 132g --executor-memory 132g josimtext_2.10-0.2.jar senses-ukwac-dep-cw-e0-N200-n200-minsize5-js-format-semeval.csv features/words-from-deps-50nn-20000.csv features/deps-10nn-20000.csv Dataset-TWSI-2-js-features.csv twsi-$mode 0.00001 $mode true 20000 16 1> $model.log 2> $mode.err ; done

spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class WSD --num-executors 16 --driver-memory 32g --executor-memory 32g josimtext_2.10-0.2.jar senses-ukwac-dep-cw-e0-N200-n200-minsize5-js-format-semeval.csv features/words-from-deps-50nn-20000.csv features/deps-10nn-20000.csv Dataset-SemEval-2013-13-js-features.csv semeval-DepsallCoocsClusters-3 0.00001 DepsallCoocsClusters true 20000 16 2> err

/home/panchenko/spark/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class WSD --num-executors 16 --driver-memory 132g --executor-memory 132g josimtext_2.10-0.2.jar senses-ukwac-dep-cw-e0-N200-n200-minsize5-semeval-twsi.csv features/words-from-deps-50nn-20000.csv features/deps-10nn-20000.csv Dataset-SemEval-2013-13-js-features.csv semeval-DepsallCoocsClusters 0.00001 DepsallCoocsClusters true 20000 16

spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class WSD --num-executors 8 --driver-memory 8g --executor-memory 8g nsi_2.10-0.0.1.jar words-from-deps-20nn-b deps-20nn-b Dataset-SemEval-2013-13-js-features.csv semeval-predict-b 0.00001 Product true
