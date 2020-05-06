import operator
from wordcloud import WordCloud
from decimal import *
import matplotlib.pyplot as plt

top_five_category = ['Restaurants', 'Shopping', 'Food', 'Home Services', 'Beauty & Spas']
num_word_cloud = 5
total = dict()
blacklist = ["else","anywhere","customer","exceptional","poor","second","third","fourth","fifth","amount","saying","spend","store","feel","grocery","service","least","already","time","taking",
"overall","experience","move","moving","let","away","tell","bathroom","desk","front","piping","notch"]
for category in top_five_category:
    words = dict()
    list_top = []
    path = category + "2/part-r-00000"
    with open(path,'r') as f:
        for line in f:
            line = line.split(",")
            new_line = line[0].split("-")
            word0 = new_line[0]
            word1 = new_line[1][1:]
            if (word0 not in blacklist) and (word1 not in blacklist):
                words[line[0]] = float(line[1])
    words = sorted(words.items(), key=operator.itemgetter(1), reverse=True)
    idx = 0
    for key in words:
        list_top.append(key)
        idx += 1
        if idx == num_word_cloud:
            break
    total[category] = list_top
getcontext().prec = 3
for category,words in total.items():
    print(category)
    s = ""
    for line in words:
        s += "& " +line[0] + " & " + '%.3f'%(line[1]) + " \\\\ \n"
    print(s)
