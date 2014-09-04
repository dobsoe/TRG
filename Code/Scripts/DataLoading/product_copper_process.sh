#!/bin/bash

hdfs dfs -rmr /data/product_copper_new

hadoop jar /usr/lib/gphd/hadoop-mapreduce-2.0.5_alpha_gphd_2_1_1_0/hadoop-streaming.jar \
        -D mapred.reduce.tasks=0 \
        -D mapred.map.tasks=40 \
        -D mapred.output.compress=true \
        -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
        -file /home/gpadmin/product_copper_mapper.py \
        -input /data/product_copper_list.txt \
        -output /data/product_copper_new \
        -mapper product_copper_mapper.py \
        -reducer /bin/cat
