
import json
import pandas as pd
import numpy as np
import operator
import nltk
import string
from nltk.corpus import stopwords
from collections import Counter


def get_df(path):
    df = pd.read_json(path)
    return df

def get_top_five_category(review_df_in, business_df_in):
    review_df = review_df_in[['business_id','text']]
    business_df = business_df_in[['business_id','categories']]

    # split categories by comma
    business_df = business_df.assign(categories=business_df.categories.str.split(',')).explode('categories').reset_index(drop=True)
    business_df["categories"] = business_df['categories'].str.strip()

    # get top five most frequent industry
    top_five_category = business_df.categories.value_counts().index.tolist()[:5]
    business_df = business_df[business_df.categories.isin(top_five_category)]
    business_df = business_df.groupby(["business_id"]).agg( ','.join).reset_index()

    return top_five_category, business_df

def main():
    review_path = 'data/selected_review.json'
    business_path = 'data/selected_business.json'
    review_df = get_df(review_path)
    business_df = get_df(business_path)

    # top_five_businss_df: "business_id", "category1";"business_id", "category2"
    top_five_category, top_five_business_df = get_top_five_category(review_df,business_df)
    print(top_five_category)
    print("finish generate df")

    total = dict()
    stop_words = stopwords.words('english')
    english_words = nltk.corpus.words.words()
    stopwords_dict = Counter(stop_words)
    english_words_dict = Counter(english_words)
    whitelist = string.ascii_letters + ' '
    for category in top_five_category:
        total[category] = []

    for index,row in top_five_business_df.iterrows():
        print(index)
        business_id = row["business_id"]
        category = row["categories"]
        category = [x.strip() for x in category.split(",")]
        temp = review_df.loc[review_df["business_id"] == business_id]
        text_values = temp['text'].values
        new_values = []
        for text_value in text_values:
            text_value = ''.join(c.lower() for c in text_value if c in whitelist)
            text_value = ' '.join([word for word in text_value.split() if (word not in stopwords_dict) and (word in english_words_dict)])
            text_value += '\n'
            new_values.append(text_value)
        for x in category:
            output_path = x + ".txt"
            file = open(output_path, 'a')
            file.writelines(new_values)
            file.close()

if __name__== "__main__":
    main()