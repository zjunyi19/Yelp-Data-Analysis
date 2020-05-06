import random
import time
from scipy import linalg
from sklearn.svm import SVC, LinearSVC
import pandas as pd
import numpy as np
from sklearn.utils import shuffle


def extract_dictionary(df):
    category_dict = {}
    k = 0
    for i in range(0, df.shape[0]):
        temp = df['categories'][i]
        if isinstance(temp, str):
            res = temp.split(",")

            for j in range(0, len(res)):
                cat = res[j]
                if cat[0] == " ":
                    cat = cat[1:]
                if cat not in category_dict:
                    category_dict[cat] = k
                    k += 1

    return category_dict


def select_classifier(penalty='l2', c=1.0, degree=1, r=0.0, class_weight='balanced'):
    if penalty == "l2":
        if degree != 2:
            return SVC(kernel="linear", degree=degree, C=c, class_weight=class_weight)
        else:
            return SVC(kernel="poly", degree=degree, coef0=r, C=c, class_weight='balanced')
    if penalty == "l1":
        return LinearSVC(penalty="l1", C=c, class_weight='balanced', dual=False)


def performance(y_true, y_pred):
    correct = 0

    for i in range(0, len(y_pred)):
        correct += abs(y_true[i] - y_pred[i])

    return correct / len(y_true)


def main():
    start_time = time.time()
    # The size of training dataset
    class_size = 1000
    outfile = open('output.csv', "w")


    # Prepare the training dataset
    training_data = 'business_review_users.csv'
    dataframe = pd.read_csv(training_data, names=['categories', 'stars', 'user_id', 'review_count',
                                                  'yelping_since', 'useful', 'funny', 'cool', 'fans', 'average_stars',
                                                  'num_friends'], header=0)
    dataframe = shuffle(dataframe)
    dataframe = dataframe[:class_size + 100].reset_index(drop=True).copy()
    # get rid of the features we are not interested in
    dataframe.pop('user_id')
    dataframe.pop('useful')
    dataframe.pop('funny')
    dataframe.pop('cool')
    # Get the ground truth
    y = dataframe.pop('stars').values
    categories = extract_dictionary(dataframe)
    cate_frame = dataframe.pop('categories')

    # Split the dataset to training part and testing part
    X_train = dataframe[:class_size].reset_index(drop=True).copy()
    X_test = dataframe[class_size:dataframe.size].reset_index(drop=True).copy()
    cate_train = cate_frame[:class_size]
    cate_test = cate_frame[class_size:].reset_index(drop=True)
    Y_train = y[:class_size]
    Y_test = y[class_size:]

    # Normalize the matrix and transform teh entries to numeric values

    # Get the first several columns of the matrix, which are the users' attributes
    part1 = X_train.to_numpy()
    for i in range(0, part1.shape[0]):
        part1[i][1] = (int)(part1[i][1][0:4])
    part1 = part1.astype(float)

    part2 = X_test.to_numpy()
    for i in range(0, part2.shape[0]):
        part2[i][1] = int(part2[i][1][0:4])
    part2 = part2.astype(float)

    # Get the resting columns of the matrix, which are the categories it belongs to
    X_feature_train = np.zeros([X_train.shape[0], len(categories)])
    X_feature_test = np.zeros([X_test.shape[0], len(categories)])
    for i in range(0, part1.shape[0]):
        if isinstance(cate_train[i], str):
            res = cate_train[i].split(",")
            for j in range(0, len(res)):
                cat = res[j]
                if cat[0] == " ":
                    cat = cat[1:]
                index = categories[cat]
                X_feature_train[i][index] = 1

    X_feature_train = np.concatenate((X_feature_train, part1), axis=1)

    for i in range(0, part2.shape[0]):
        if isinstance(cate_test[i], str):
            res = cate_test[i].split(",")
            for j in range(0, len(res)):
                cat = res[j]
                if cat[0] == " ":
                    cat = cat[1:]
                index = categories[cat]
                X_feature_test[i][index] = 1

    X_feature_test = np.concatenate((X_feature_test, part2), axis=1)

    # Preparation finishes

    # SVD
    U, s, Vh = linalg.svd(X_feature_test, full_matrices=False)
    sum = 0
    total = np.sum(s ** 2)
    i = 0
    for i in range(0, s.shape[0]):
        if sum + s[i] ** 2 >= total * 0.8:
            break
        else:
            sum += s[i] ** 2
    X_test_reconstruct = np.zeros(X_feature_test.shape)
    for j in range(0, i + 1):
        X_test_reconstruct = X_test_reconstruct + s[j] * (np.outer(U[:, j], Vh[j]))
    X_feature_test = X_test_reconstruct.copy()

    U, s, Vh = linalg.svd(X_feature_train, full_matrices=False)
    sum = 0
    total = np.sum(s ** 2)
    i = 0
    for i in range(0, s.shape[0]):
        if sum + s[i] ** 2 >= total * 0.8:
            break
        else:
            sum += s[i] ** 2
    X_train_reconstruct = np.zeros(X_feature_train.shape)
    for j in range(0, i + 1):
        X_train_reconstruct = X_train_reconstruct + s[j] * (np.outer(U[:, j], Vh[j]))
    X_feature_train = X_train_reconstruct.copy()
    # SVD Finishes

    # Train the model on different kernels and parameters
    # kenels which are not chosen are transformed to comments
    C_range = [0.001]
    Cr_range = []
    for i in range(25):
        lgc = random.uniform(-3, 3)
        lgr = random.uniform(-3, 3)
        Cr_range.append([10 ** lgc, 10 ** lgr])
    max = 10
    print("rbf")
    for c, r in Cr_range:
        print('c=' + str(c) + "; r=" + str(r))
        clf = SVC(kernel='rbf', random_state=0, gamma=r, C=c)
        clf.fit(X_feature_train, Y_train)
        y_predict = clf.predict(X_feature_test)
        perf = performance(Y_test, y_predict)
        print(perf)
        if perf < max:
            max = perf
    print(max)
    outfile.write(str(max) + '\n')
    # outfile.write("l2, degree=1\n")
    # for c in C_range:
    #     clf = select_classifier('l2', c, degree=1)
    #     clf.fit(X_feature_train, Y_train)
    #     y_predict = clf.predict(X_feature_test)
    #     perf = performance(Y_test, y_predict)
    #     print(perf)
    #     if perf < max:
    #         max = perf
    # outfile.write(str(max) + '\n')
    # print(max)
    #
    # print("poly")
    # for c,r in Cr_range:
    #     print('c='+str(c)+"; r="+str(r))
    #     clf = select_classifier(c=c, degree=2, r=r)
    #     clf.fit(X_feature_train, Y_train)
    #     y_predict = clf.predict(X_feature_test)
    #     perf = performance(Y_test, y_predict)
    #     print(perf)
    #

    # max=10
    # print("sigmoid")
    # for c in C_range:
    #     print('c=' + str(c))
    #     clf = SVC(kernel='sigmoid', gamma=c)
    #     clf.fit(X_feature_train, Y_train)
    #     y_predict = clf.predict(X_feature_test)
    #     perf = performance(Y_test, y_predict)
    #     print(perf)
    #     if perf < max:
    #         max = perf
    # print(max)
    # outfile.write(str(max)+'\n')

    end_time = time.time()
    print(end_time - start_time)


if __name__ == "__main__":
    main()
