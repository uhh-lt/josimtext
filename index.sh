for i in {0..9} ; do
    for j in {0..9} ; do
        part="0$i$j"
        time ./run StoreToElasticSearch \
            "corpora/depcc-conll/part-m-$part*" \
            "depcc5/sentences" \
            "ltheadnode,ltnode1a,ltnode2a,ltnode3a,ltnode4a" \
            1 \
            1000 \
            $part \
            &> "$part.log" 
    done
done
