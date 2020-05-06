import operator
from wordcloud import WordCloud
import matplotlib.pyplot as plt

top_five_category = ['Restaurants', 'Shopping', 'Food', 'Home Services', 'Beauty & Spas']
num_word_cloud = 30
total = dict()
blacklist = ["go","have","inner","help","experience","side","around","table","small","night","dinner","lunch","shopping","having","order","us","would","good","great","bad","one","two","also","really","even","dont","back","place","like","restaurant","food","try","love","company","nice","home","recommend","terrible","going","everything","first","well","delicious","ordered","get","lot","happy","new","know","knew","use","many","next","since","give","getting","super","another","last","work","looking",
"always","going","went","think","sure","got","came","come","people","can","will","could","every","ever","right","much","take","took","definitely","bit","wasnt","times","best","done","check","something","want","need","way","make","made","tried","amazing","little","cant","never","days","didnt","someone","say","said","pretty","told","shop","day","see","buy","found","better","still","coming","left","highly","anything","bought","different","long","job"]
for category in top_five_category:
    words = dict()
    list_top = []
    path = category + "/part-r-00000"
    with open(path,'r') as f:
        for line in f:
            line = line.split(",")
            if line[0] not in blacklist:
                words[line[0]] = float(line[1])
    words = sorted(words.items(), key=operator.itemgetter(1), reverse=True)
    idx = 0
    for key in words:
        list_top.append(key[0])
        idx += 1
        if idx == num_word_cloud:
            break
    total[category] = list_top

for category,words in total.items():
    print(category)
    s = ""
    for word in words:
        s += word + ", "
    print(s)

for category in top_five_category:
    text = " ".join(total[category])
    wc = WordCloud(width = 800, height = 800,
                background_color ='white',
                min_font_size = 8).generate(text)
    path = category + ".png"
    wc.to_file(path)
