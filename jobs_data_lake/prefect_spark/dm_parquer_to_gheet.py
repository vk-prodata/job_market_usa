# from pydrive.auth import GoogleAuth
# from pydrive.drive import GoogleDrive
# import gspread
# from oauth2client.service_account import ServiceAccountCredentials
# import gspread_dataframe as gd
# from pyspark.sql import SparkSession
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import pandas as pd
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
# def read_gsheet():
#     scopes = ['https://www.googleapis.com/auth/spreadsheets',
#             'https://www.googleapis.com/auth/drive']

#     credentials = Credentials.from_service_account_file('credentials/dtc-de-375803-8dd8e44e7ffd.json', scopes=scopes)

#     gc = gspread.authorize(credentials)

#     gauth = GoogleAuth()
#     drive = GoogleDrive(gauth)
#     sheet_id = '1Vd9U3pNQGguPjmxkDZKq5DD6kTB6Mwy08bpt2WICTiU'
#     # open a google sheet
#     gs = gc.open_by_key(sheet_id)
#     # select a work sheet from its name
#     sheet = gs.worksheet('staging')
#     print(sheet)

def write_job_count():

    scopes = ['https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive']

    credentials = Credentials.from_service_account_file('credentials/dtc-de-375803-8dd8e44e7ffd.json', scopes=scopes)

    gc = gspread.authorize(credentials)

    gauth = GoogleAuth()
    drive = GoogleDrive(gauth)
    sheet_id = '1Vd9U3pNQGguPjmxkDZKq5DD6kTB6Mwy08bpt2WICTiU'
    # open a google sheet
    gs = gc.open_by_key(sheet_id)
    # select a work sheet from its name
    sheet = gs.worksheet('staging')

    # dataframe (create or import it)
    df = pd.DataFrame({'a': ['apple','airplane','alligator'], 'b': ['banana', 'ball', 'butterfly'], 'c': ['cantaloupe', 'crane', 'cat']})
    # write to dataframe
    sheet.clear()
    set_with_dataframe(worksheet=sheet, dataframe=df, include_index=False,
    include_column_header=True, resize=True)

if __name__ == "__main__":
    write_job_count()