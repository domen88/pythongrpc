import pandas as pd
import numpy as np

supervised = pd.DataFrame()
train_set = pd.DataFrame()
columns = []


class Support:

    @staticmethod
    def calculate(filename):
        df = pd.read_pickle(filename)
        print(df)

    @staticmethod
    def setup():
        global train_set, supervised, columns

        print('SETUP MODELLO')
        # leggo il csv
        scania_train_url ='aps_failure_training_set.csv'
        scania_train = pd.read_csv(scania_train_url, header=14, na_values='na')

        # sostituisco i missing values con la media della colonna
        scania_train.fillna(scania_train['ab_000'].mean(), inplace=True)

        # filtro gli attributi corrispondenti agli indici ricevuti dal cloud
        scania_train = scania_train[np.append(['class'], columns)]

        # sostituisco i valori della classe con interi
        scania_train['class'].replace(to_replace=dict(pos=1, neg=0), inplace=True)

        # converto tutti i float in interi
        scania_train = scania_train.astype('int64')

        # salvo le ultime 58k istanze come dati supervisionati
        supervised = scania_train.iloc[2000:31000, :]

        # salvo le prime 2k istanze utilizzate per costruire il modello iniziale
        train_set = scania_train.head(2000)

