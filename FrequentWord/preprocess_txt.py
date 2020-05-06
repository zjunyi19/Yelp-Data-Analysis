import nltk

from nltk.stem import SnowballStemmer
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize

top_five_category = ['Restaurants', 'Shopping', 'Food', 'Home Services', 'Beauty & Spas']
blacklist = ["go","have","shopping","having","order","us","would","good","great","bad","one","two","also","really","even","dont","back","place","like","restaurant","food","try","love","company","nice","home","recommend","terrible","going","everything","first","well","delicious","ordered","get","lot","happy","new","know","knew","use","many","next","since","give","getting","super","another","last","work","looking","able","aa","absolutely","actually","ago","almost","new","complete","zero","yet","yelp",
"always","going","went","think","sure","got","came","come","people","can","will","could","every","ever","right","much","take","took","definitely","bit","wasnt","times","best","done","check","something","want","need","way","make","made","tried","amazing","little","cant","never","days","didnt","someone","say","said","pretty","told","shop","day","see","buy","found","better","still","coming","left","highly","anything","bought","different","long","job"]
for category in top_five_category:
    print(category)
    path = category + ".txt"
    outputPath = category + "_new.txt"
    new_text = []
    with open(path,'r') as f:
        for line in f:
            tokens = nltk.word_tokenize(line)
            tokens = [word for word in tokens if word not in blacklist]
            new_line = " ".join(tokens)
            new_line += '\n'
            new_text.append(new_line)
    file = open(outputPath,'w')
    file.writelines(new_text)
    file.close()

