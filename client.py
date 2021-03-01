import pandas as pd
import sys
import lib
import time


def main(argv):

    if len(sys.argv) <= 1:
        print(sys.stderr, 'Usage python ' + sys.argv[0] + ' [nline]')
        # Exit with error code
        sys.exit(1)

    line = int(argv[0])

    client = lib.FileClient('localhost:9000')

    # leggo il csv
    nRowsRead = None  # specify 'None' if want to read whole file
    file_name = './gas-sensor-array-temperature-modulation/20161005_140846.csv'
    df = pd.read_csv(file_name, delimiter=',', nrows=nRowsRead)
    number_of_rows = len(df.index)
    n_packets = int(number_of_rows / line) + 1

    # loop per leggere nrighe alla volta accorparle e inviarle
    i = 0
    for x in range(n_packets):
        df1 = df.iloc[i*line:(i+1)*line]
        in_file_name = 'row' + str(i) + '.zip'
        i = i + 1
        df1.to_pickle(in_file_name, compression='zip')
        rep = client.upload(in_file_name)
        print(rep)
        time.sleep(10)


if __name__ == '__main__':
    main(sys.argv[1:])
