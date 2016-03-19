import csv, nltk
import pandas as pd
import numpy as np
from collections import defaultdict
from nltk.stem.lancaster import LancasterStemmer #this is the more aggressive stemmer
from nltk.stem import WordNetLemmatizer
stemmed = LancasterStemmer()
lemmitized = WordNetLemmatizer()

FilePath = "/Users/ericadretzka/Desktop/Programming/DDL Research Lab/"
FileName = "DDL topicmaps.csv"
File = FilePath + FileName

#create empty list which will hold all topics of interest to the DDL community
AllTopics = []
AllTopics_Stemmed = []

#-------------------------------------
#(A) Create dataframe
#-------------------------------------
columnsDF = ["Interest Count"]
#indicesDF = ["List_DataScience", "List_StatisticsBasic", "List_StatisticsAlgor", "List_DataScience", "List_R", "List_Python", "List_Languages", "List_Visualization", "List_Ingestion", "List_Architecture", "List_Munging", "List_BigData", "List_GIS"]
indicesDF = defaultdict(list)

#create lists of topics for supervised-style classifications, appending each to the dictionary
indicesDF["List_DataScience"] = ["pandas", "science", "unsupervised"]
indicesDF["List_StatisticsBasic"] = ["statistic", "regression", "unsupervised", "time-series", "algorithm"]
indicesDF["List_StatisticsAlgor"] = ["statistic", "regression", "unsupervised", "time-series", "cluster", "deep learn", "neural", "algorithm"]
indicesDF["List_DataScience"] = ["tree", "spatial", "gis", "forest", "model", "simulation", "time-series", "cluster"]
indicesDF["List_R"] = ["r", "ggplot", "shiny"]
indicesDF["List_Python"] = ["pandas", "python", "numpy"]
indicesDF["List_Languages"] = ["c+", "c#", "python", "r", ]
indicesDF["List_Visualization"] = ["shiny", "tableau", "dashboard", "visual"]
indicesDF["List_Ingestion"] = [""]
indicesDF["List_Architecture"] = ["architecture", "unix", "spark", "hadoop"]
indicesDF["List_Munging"] = ["mung", "clean", "bugs", "errors"]
indicesDF["List_BigData"] = ["big data", "spark", "hadoop"]
indicesDF["List_GIS"] = ["spatial", "gis", "geosp"]
indicesDF["List_Microsoft"] = ["excel", "powerpoint", "word"]
indicesDF["List_Networks"] = []

#create pandas dataframe with indices labeled from keys of indicesDF dictionary
ClassifiedTopics = pd.DataFrame(index = indicesDF.keys(), columns = columnsDF).fillna(value=0)
print ClassifiedTopics

#-------------------------------------
#(B) Loop through each line of DDL data, normalizing the words
#-------------------------------------
#open file, create list with all topics in it
with open(File, 'rU') as TopicMap_Orig:
    TopicMap = csv.reader(TopicMap_Orig, delimiter = ',')
    for Topic in TopicMap:
        AllTopics.append(Topic[1])
        #by stemming the entries we're able to reconcile better; populate list
        ProcessedWords = lemmitized.lemmatize(stemmed.stem(Topic[1]))
        AllTopics_Stemmed.append(stemmed.stem(ProcessedWords))

#convert to set to get unique entries
#there are 660 entries total; set reduces this to the unique entries, a total of 272 (277 non-stemmed)
UniqueTopics = set(AllTopics_Stemmed)

#-------------------------------------
#(C) Loop through the DDL file, counting frequency of topics of interest inside greater topic categories
#-------------------------------------
#supervised style
for topic in UniqueTopics:                  #loop through each topic in the DDL topicmap
    for indice in indicesDF.keys():                #loop through each topic category as defined by the lists above
        print indice
        print indicesDF[indice]
        for bucketedTopic in indicesDF[indice]:
            print bucketedTopic
            isPresent = topic.find(bucketedTopic)      #check if topic exists in the category
            print isPresent
            if isPresent > -1:
                print topic + " " + indice
                ClassifiedTopics["Interest Count"][indice] = ClassifiedTopics["Interest Count"][indice] + 1

print ClassifiedTopics