
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

    return top_five_category, business_df

def main():
    review_path = 'data/selected_review.json'
    business_path = 'data/selected_business.json'
    review_df = get_df(review_path)
    business_df = get_df(business_path)

    # top_five_businss_df: "business_id", "category1";"business_id", "category2"
    top_five_category, top_five_business_df = get_top_five_category(review_df,business_df)
    print(top_five_business_df)
    business_id_list = top_five_business_df.business_id.unique()
    review_star = review_df[review_df.business_id.isin(business_id_list)]
    review_star = review_star[["business_id","stars"]]

    bus_re_df = pd.merge(review_star, top_five_business_df, how='inner', on='business_id')[['categories','stars']]
    print(bus_re_df)

    bus_re_df.to_csv (r'category_star.csv', index = False, header=False)

    # print("finish dictionary")
    # for category, texts in total.items():
    #     output_path = category + ".txt"
    #     file = open(output_path, 'w')
    #     file.writelines(texts)
    #     file.close()





if __name__== "__main__":
    main()