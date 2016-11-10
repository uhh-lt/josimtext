aws emr create-cluster \
    --applications Name=Hadoop \
    --ec2-attributes \
        '{"KeyName":"your-keypair-name", \                                         <---- change this
        "InstanceProfile":"EMR_EC2_DefaultRole", \
        "SubnetId":"subnet-xxxxx", \                                               <---- change this
        "EmrManagedSlaveSecurityGroup":"sg-xxxxxx", \                              <---- change this
        "EmrManagedMasterSecurityGroup":"sg-xxxxxx"}' \                            <---- change this
    --service-role EMR_DefaultRole \
    --enable-debugging \
    --release-label emr-4.2.0 \
    --log-uri 's3n://your-logs/elasticmapreduce/' \                                <---- change this
    --steps '[\
        {"Args":["de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.full.Phase1FullJob", \
        "-D","mapreduce.task.timeout=7200000", \
        "-D", "mapreduce.map.failures.maxpercent=5", \
        "-D", "mapreduce.map.maxattempts=2", \
        "-D", "c4corpus.keepminimalhtml=true", \                      <---- change this (optionally)
        "s3://commoncrawl/crawl-data/CC-MAIN-2015-27/segments/*/warc/*.warc.gz",\
        "s3://ukp-research-data/c4corpus/cc-phase1out-2015-11"], \                 <---- change this
        "Type":"CUSTOM_JAR", \
        "ActionOnFailure":"CANCEL_AND_WAIT", \
        "Jar":"s3://path-to-your/dkpro-c4corpus-hadoop-1.0.0.jar", \               <---- change this
        "Properties":"", \
        "Name":"Custom JAR"}]' \
    --name 'Full cluster phase 1' \
    --instance-groups '[\
        {"InstanceCount":32, \                                        <---- change this (optionally)
            "bid-price":"your-bid-value", \                                        <---- change this
            "InstanceGroupType":"TASK",\
            "InstanceType":"c3.8xlarge", \
            "Name":"c3.8xlarge = 32 CPU"}, \
        {"InstanceCount":2, \
            "InstanceGroupType":"CORE",\
            "InstanceType":"m3.xlarge", \
            "Name":"Core instance group - 2"}, \
        {"InstanceCount":1, \
            "InstanceGroupType":"MASTER", \
            "InstanceType":"m3.xlarge", \
            "Name":"Master instance group - 1"}]' \
    --auto-terminate \
    --region us-east-1
