import csv, nltk, dedupe, logging, optparse, re
from unidecode import unidecode
import pandas as pd
import numpy as np
from collections import defaultdict
from nltk.stem.lancaster import LancasterStemmer #this is the more aggressive stemmer
from nltk.stem import WordNetLemmatizer
stemmed = LancasterStemmer()
lemmitized = WordNetLemmatizer()

FilePath = "/Users/ericadretzka/Desktop/Programming/DDL Research Lab/DDLTopics/"
FileName = "DDL topicmaps.csv"
InputFile = FilePath + FileName
TrainingFile = "trainingResults1.json"
OutputFile = "DDLTopics Results1.csv"

#-------------------------------------
#(A) Pre-process data
#-------------------------------------
def preProcess(column):
    import unidecode
    column = unidecode.unidecode(column)
    column = lemmitized.lemmatize(stemmed.stem(column))
    return column

#-------------------------------------
#(B) Loop through each line of DDL data, normalizing the words
#-------------------------------------
#open file, create list with all topics in it
def readData(InputFile):
    TopicMaps = {}
    Count = 1
    with open(InputFile, 'rU') as TopicMap_Orig:
        TopicMap = csv.DictReader(TopicMap_Orig)
        for Topic in TopicMap:
            RowClean = [(k, preProcess(v)) for (k, v) in Topic.items()]
            TopicMaps[Count] = dict(RowClean)

            Count = Count + 1
    return TopicMaps

TopicMap_Proc = readData(InputFile)

#-------------------------------------
#(C) Assemble training set & run through the training model
#-------------------------------------
#select which fields are to be examined
fields = [{'field': 'term', 'type': 'String'}]

deduper = dedupe.Dedupe(fields)
deduper.sample(TopicMap_Proc, 150)

#now, run through samples for training the model & save the training data to a file
dedupe.consoleLabel(deduper)
deduper.train()
with open(TrainingFile, 'wb') as TrainResult:
    deduper.writeTraining(TrainResult)

#-------------------------------------
#(D) Cluster results, resulting in blocking
#-------------------------------------
#find threshold that maximizes weighted average of precision & recall
# by setting the weight at 2 we're saying we care twice as much about recall as precision
# since this is a small dataset we can pass 100% of the data in, otherwise we'd just do a representative sample
threshold = deduper.threshold(TopicMap_Proc, recall_weight = 2)

#match returns sets of record IDs that Dedupe believes are referring to the same entity
clustered_dupes = deduper.match(TopicMap_Proc, threshold)

#-------------------------------------
#(E) Write original results back to file with new column indicating its newly-assigned cluster
#-------------------------------------
TopicCluster = {}
ClusterID = 0
for (ClusterID, cluster) in enumerate(clustered_dupes):
    id_set, scores = cluster
    cluster_d = [TopicMap_Proc[c] for c in id_set]
    canonical_rep = dedupe.canonicalize(cluster_d)
    for record_id, score in zip(id_set, scores):
        TopicCluster[record_id] = {
        "Cluster ID": ClusterID,
        "Canonical Representation" : canonical_rep,
        "Confidence" : score
        }
        print TopicCluster[record_id]
singleton_id = cluster_id + 1

with open(output_file, 'w') as f_output, open(input_file) as f_input:           #establish output & input file
    writer = csv.writer(f_output)
    reader = csv.reader(f_input)

    heading_row = next(reader)                      #skip header row
    heading_row.insert(0, 'confidence_score')       #add new column to capture confidence score
    heading_row.insert(0, 'Cluster ID')             #add new column to capture the cluster ID
    canonical_keys = canonical_rep.keys()
    for key in canonical_keys:
        heading_row.append('canonical_' + key)      #add new columns for canonical versions of original columns

    writer.writerow(heading_row)

    for row in reader:
        print row
        row_id = int(row[0])
        if row_id in TopicCluster:
            cluster_id = TopicCluster[row_id]["Cluster ID"]
            canonical_rep = TopicCluster[row_id]["Canonical Representation"]
            row.insert(0, TopicCluster[row_id]['Confidence'])
            for key in canonical_keys:
                row.append(canonical_rep[key].encode('utf-8'))
            else:
                row.insert(0, None)
                row.insert(0, singletonID)
                singletonID += 1
                for key in canonical_keys:
                    row.append(None)
            writer.writerow(row)