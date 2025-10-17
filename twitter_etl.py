import pandas as pd
def run_twitter_etl():
    # Read the CSV file into a DataFrame
    df = pd.read_csv("tweetsData/tweets.csv")
    df.to_csv("elonmusk_twitter_data.csv")
    # print(df.head())

    # print(df.columns)