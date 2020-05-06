import json
import pandas as pd
import numpy as np

def read_data(path,output_path,prob):
    saved_data = []
    with open(path,buffering = 2000000) as f:
        for line in f:
            coin = np.random.choice(np.arange(0, 2), p=[1-prob, prob])
            if(coin == 1):
                data = json.loads(line)
                saved_data.append(data)
    with open(output_path,"w") as outfile:
        json.dump(saved_data, outfile)


def main():
    user_path = 'data/yelp_academic_dataset_user.json'
    review_path = 'data/yelp_academic_dataset_review.json'
    business_path = 'data/yelp_academic_dataset_business.json'
    output_user = 'data/selected_user.json'
    output_review = 'data/selected_review.json'
    output_business = 'data/selected_business.json'
    read_data(user_path,output_user,0.7)
    read_data(review_path,output_review,0.8)
    read_data(business_path,output_business,1)

if __name__== "__main__":
    main()
