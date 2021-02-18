import pandas as pd
import os
import lib
import time


def main():

    client = lib.FileClient('localhost:9000')

    # leggo il csv
    #scania_train_url = 'https://archive.ics.uci.edu/ml/machine-learning-databases/00421/aps_failure_training_set.csv'
    scania_train_url ='aps_failure_training_set.csv'
    scania_train = pd.read_csv(scania_train_url, header=14, na_values='na')

    # sostituisco i missing values con la media della colonna
    scania_train.fillna(scania_train['ab_000'].mean(), inplace=True)

    #print(scania_train)

    # utilizzo le ultime 58k istanze del dataset come se fossero nuovi dati da classificare
    new_istances = scania_train.drop('class', axis=1).iloc[2000:31000, :]


    #print(new_istances)
    i = 0
    for row in new_istances.itertuples(index=False):
        df = pd.DataFrame(row)
        in_file_name = 'row' + str(i) + '.zip'
        i = i + 1
        df.to_pickle(in_file_name, compression='zip')
        rep = client.upload(in_file_name)
        print(rep)
        time.sleep(60)


if __name__ == '__main__':
    main()
