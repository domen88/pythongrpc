import lib
import pandas as pd
from al import Support


def main():
    i = 0
    Support.setup()
    while True:
        lib.FileServer().start(9000, i)
        #file_name = 'server_row' + str(i) + '.zip'
        i = i + 1


if __name__ == '__main__':
    main()