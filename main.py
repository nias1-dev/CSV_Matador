import pandas as pd
from dotenv import load_dotenv
import os
import email_builder_testing as emailer #local method under development
import upload_to_s3 as uploader
import time


"""
 Script calculate sales total on days that equal tues from sales tracked on csv. 
Sales campaign rewarding seller with most sales on Tuesdays. Saves to s3 bucket with server side encryption and 7 day delete rule. 

Script considers Security by employing environment variables to keep sensitive data private. 

Completed 1/2 by A.S.III"""


load_dotenv()

def create_df(csv):
    """reads csv file and saves to pandas dataframe. returns pandas df for later manipulation by other programs"""
    df = pd.read_csv(csv)
    calculate_tuesday_sales(df)
    calculate_all_sales_dashboard(df)
    time.sleep(3)
    save_winner_s3_aws()  # calls local function to write to CSVs to S3.
    return df


def csv_facts(df):
    """pulls facts about csv as in names of all seller, total rows. Function available to expand on future analytics needs"""
    print("shape: ",df.shape, '\n'*2)
    print("checking null: ")
    print(df.isnull().sum())
    print('\n' * 2)
    print("head: --> ", '\n'*2)
    print(df.head())
    print("most frequent seller: --> ", '\n'*2)
    top_frequency = df['Sales_Person'].mode()[0]
    top_frequency_count = df['Sales_Person'].value_counts().max()
    lowest_frequency_count = df['Sales_Person'].value_counts().min()
    print("most entries:", top_frequency, top_frequency_count, '\n'*2)


def calculate_tuesday_sales(df):
    """calculates alls sales from tuesdays. Determines winning Sales Person and Total amount of sales for winner. Saves results to CSV."""
    tuesday_sales = df[df['Day_of_Week'] == 'Tuesday'] #filter sales to tuesday
    #creates pivot table that ranks sellers by sales, includes sellers email address as field
    tuesday_table = pd.pivot_table(tuesday_sales,values='Sale_Amount',index=['Sales_Person','Email'], aggfunc='sum')
    print(tuesday_table, end='\n\n')

    #preforms independent aggregate and selects highest seller to determine winner.
    top_seller = (
    tuesday_sales
    .groupby('Email', as_index=True)
    .agg(total_sales=('Sale_Amount', 'sum'))
    )

    #writes winner dataframe to csv
    top_seller_csv = (
        top_seller
        .sort_values('total_sales', ascending=False)
        .head(1)
    )
    top_seller_csv.to_csv("Data/" + os.getenv('WINNER_SECRET_CSV'))  # creates csv S3 functions will use to save to cloud.

    #captures winner as simple string of email
    top_seller = (
        tuesday_sales
        .groupby('Email')['Sale_Amount']
        .sum()
        .idxmax()
    )

    return top_seller


def calculate_all_sales_dashboard(df):
    """calculates all sales in pivot. Saves to new csv. to be consumed in webapp as dashboard."""
    dash = pd.pivot_table(df, values='Sale_Amount', index=['Sales_Person', 'Email'], aggfunc='sum')
    dash.to_csv("Data/" + os.getenv('DASHBOARD_SECRET_CSV'))


def save_winner_s3_aws():
    """function collects csvs and saves them to s3. Deletes local csv set once complete"""
    uploader.save_sheets_to_s3()


if __name__ == '__main__':
    csv_file = os.getenv('CSV_SECRET')
    data = create_df(csv_file)


