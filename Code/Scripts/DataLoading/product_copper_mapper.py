#!/usr/bin/env python

import sys
import os
import csv
import gzip
import subprocess

def smart_next(f):
        r = None
        try:
                r = f.next()
        except:
                pass
        return r

pid = os.getpid()
for filename in sys.stdin:
    if filename.strip():
        cp = subprocess.Popen("hdfs dfs -get %s /tmp/temp_%d.gz" % (filename.strip(), pid), shell=True)
        cp.communicate()
        f = gzip.open('/tmp/temp_%d.gz' % pid, 'r')
        filt = (line.replace('\r', '\\r') for line in f)
        r = csv.reader(filt, delimiter='\t')
        w = csv.writer(sys.stdout, delimiter='\t', lineterminator='\n')
        line = smart_next(r)
        fieldnum = len(line)
        while line is not None:
            #print line
            line2 = smart_next(r)
            if len(line) == fieldnum and (line2 is None or line2[0][:2] == '20'):
                w.writerow(line)
            else:
                while line2 is not None and line2[0][:2] <> '20':
                    line[-1] += '\\n' + line2[0]
                    if len(line2) > 1:
                        for x in line2[1:]:
                            line.append(x)
                    line2 = smart_next(r)
                w.writerow(line[:fieldnum])
            line = line2
        f.close()
        cp = subprocess.Popen("rm /tmp/temp_%d.gz" % pid, shell=True)
        cp.communicate()
