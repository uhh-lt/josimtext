# yarn
spark-submit --class WSD --master=yarn-cluster --queue=longrunning --num-executors 20 --driver-memory 7g --executor-memory 7g ~/noun-sense-induction-scala/bin/spark/nsi_2.10-0.0.1.jar ukwac/agg/words-from-deps-20nn-b ukwac/agg/deps-20nn-b ukwac/Dataset-SemEval-2013-13-js-features.csv ukwac/semeval/1 0.00001 Product true

# test local
./spark-submit --class WSD --num-executors 8 --driver-memory 8g --executor-memory 8g ~/work/joint/wsi/nsi-scala/target/scala-2.10/nsi_2.10-0.0.1.jar /Users/sasha/Desktop/debug/wsd/words-from-deps-20nn-b /Users/sasha/Desktop/debug/wsd/deps-20nn-b /Users/sasha/Desktop/debug/wsd/Dataset-SemEval-2013-13-js-features.csv /Users/sasha/Desktop/debug/wsd/semeval-predict-b 0.00001 Product true
