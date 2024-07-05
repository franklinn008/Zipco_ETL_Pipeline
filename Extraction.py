import pandas as pd
#Data Extration
#using try and catch exception
def run_extraction():
    try:
        data = pd.read_csv('zipco_transaction.csv')
        print('data extracted succesfully')
    except Exception as e:
        print(f"{e} error occurred:")
        print('extraction succesfully')

