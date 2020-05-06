import json
import pandas as pd
import numpy as np
import operator

def get_df(path):
    df = pd.read_json(path)
    return df

def get_business_review_user_csv(review_df_in,user_df_in):
    review_df = review_df_in[['business_id','stars','user_id']]
    # business_df = business_df_in[['business_id','categories']]
    user_df=user_df_in[['user_id','review_count','yelping_since','useful','funny','cool','fans','average_stars','friends']]
    # split categories by comma
    # business_df = business_df.assign(categories=business_df.categories.str.split(',')).explode('categories').reset_index(drop=True)
    # business_df["categories"] = business_df['categories'].str.strip()

    # use number of friends as attributes

    num_friends=[]
    for i in range (0,user_df.shape[0]):
        temp=user_df['friends'][i]
        num_friends.append(temp.count(",") + 1)

    user_df = user_df.assign(num_friends=num_friends)
    # user_df['num_friends']=num_friends
    user_df.pop('friends')
    user_df.to_csv(r'users_info.csv', index=False, header=True)
    # inner join review_df and business_df
    # only keep categories and text columns
    # bus_re_df = pd.merge(business_df, review_df, how='inner', on='business_id')[['business_id','user_id','stars']]
    # bus_re_us_df=pd.merge(bus_re_df, user_df, how='inner', on='user_id')
    # save to csv file
    # bus_re_us_df.to_csv (r'business_review_users.csv', index = False, header=True)
    review_df.to_csv(r'business_users.csv', index = False, header=True)
    # user_df=user_df_in[['user_id','friends']]
    # user_df.to_csv(r'users.csv', index = False, header=True)
def main():
    user_path = 'data/selected_user.json'
    review_path = 'data/selected_review.json'
    # business_path = 'data/selected_business.json'
    review_df = get_df(review_path)
    # business_df = get_df(business_path)
    user_df=get_df(user_path)
    get_business_review_user_csv(review_df,user_df)


if __name__== "__main__":
    main()
