import pandas as pd
import numpy as np
from sklearn.utils import shuffle


def distance(x, y):
    temp = x - y
    sum = 0
    for index in temp:
        sum += abs(index)
    return sum


def main():
    pred_info = pd.read_csv('output.csv', names=['business_id', 'user_id'], header=0)
    list=[]
    for ii in range(0,pred_info.shape[0]):
        # Read the data
        uid=pred_info['user_id'][ii]
        bid=pred_info['business_id'][ii]
        user_info = pd.read_csv('users_info.csv', names=['user_id', 'review_count',
                                                         'yelping_since', 'useful', 'funny', 'cool', 'fans',
                                                         'average_stars',
                                                         'num_friends'], header=0)
        rate_info = pd.read_csv('review_test.csv', names=['business_id', 'stars', 'user_id'], header=0)

        # Find the users who reviewed on the business to be predicted
        dict_rating = dict()
        for i in range(0, rate_info.shape[0]):
            if rate_info['business_id'][i] == bid and rate_info['user_id'][i] != uid:
                dict_rating[rate_info['user_id'][i]] = rate_info['stars'][i]

        knn = 10
        newDF = pd.DataFrame()
        user_list = []
        uinfo = np.zeros(9)
        # get the attributes of those users
        for i in range(0, user_info.shape[0]):
            if user_info['user_id'][i] == uid:
                temp = int(user_info['yelping_since'][i][0:4])
                user_info['yelping_since'][i] = temp
                uinfo = user_info.iloc[i, :]
            if user_info['user_id'][i] in dict_rating:
                user_list.append(user_info['user_id'][i])
                temp = int(user_info['yelping_since'][i][0:4])
                user_info['yelping_since'][i] = temp
                newDF = newDF.append(user_info.iloc[i, :], ignore_index=True)
        # Remove the user_id in the feature, and turn it into a np matrix
        newDF.pop('user_id')
        uinfo.pop('user_id')
        uinfo = uinfo.to_numpy()
        user_mat = newDF.to_numpy()

        # normalize the feature matrix
        avg = np.average(user_mat, axis=0)
        for i in range(0, user_mat.shape[1]):
            user_mat[:, i] -= avg[i]
            user_mat[:, i] /= np.std(user_mat[:, i])

        # Calculate the distance and find the N th closest users
        distance_list = []
        for j in range(0, user_mat.shape[0]):
            distance_list.append((j, distance(uinfo, user_mat[j])))
        distance_list.sort(key=lambda x: x[1])
        # Take the average value of them
        k = 0
        sum = 0
        for j in distance_list:
            close_user_id = user_list[j[0]]
            if close_user_id in dict_rating:
                sum += dict_rating[close_user_id]
                k += 1
            if k >= knn:
                break
        print(sum / k)
        list.append(sum / k)
    sum=0
    for x in list:
        sum+=x
    print("avg error is:")
    print(list)

if __name__ == "__main__":
    main()
