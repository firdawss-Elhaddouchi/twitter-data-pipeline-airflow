import pandas as pd
import os

DATA_PATH = "/opt/airflow/data"


def run_twitter_etl():
    # Read the CSV file into a DataFrame
    df = pd.read_csv("data/tweets.csv")
    df.to_csv("elonmusk_twitter_data.csv")
    # print(df.head())

    # print(df.columns)

def extract_data():
    df = pd.read_csv(os.path.join(DATA_PATH, "tweets.csv"))
    df.to_csv(os.path.join(DATA_PATH,"extracted.csv"),index=False)
    print(" Étape 1 : Extraction terminée")
    return "Extraction OK"


def transform_data():
    df = pd.read_csv(os.path.join(DATA_PATH, "extracted.csv"))
    df= df.dropna(subset=['tweet'])
    df["tweet_length"] = df['tweet'].apply(len)
    df.to_csv(os.path.join(DATA_PATH, "transformed.csv"), index=False)
    print("✅ Étape 2 : Transformation terminée")
    return "Transformation OK"

def load_data():
    df = pd.read_csv(os.path.join(DATA_PATH, "transformed.csv"))
    df.to_csv(os.path.join(DATA_PATH, "cleaned_tweets.csv"), index=False)
    print("✅ Étape 3 : Chargement terminé")
    return "Chargement OK"
