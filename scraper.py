import pandas as pd
import numpy as np
import string
import re
from gensim.utils import simple_preprocess
import nltk
from nltk.corpus import stopwords
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
from google_play_scraper import reviews, Sort
from datetime import datetime, timedelta
from pymongo import MongoClient
import joblib
from tqdm import tqdm
# Function to establish a MongoDB connection
def connect_to_mongodb(uri, db_name, collection_name):
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    return collection



# Function to preprocess text
def preprocess_text(text):
    text = re.sub('[%s]' % re.escape(string.punctuation), '', text)
    text = re.sub('\d+', '', text)
    tokens = simple_preprocess(text)
    tokens = [token for token in tokens if token not in stop_words]
    tokens = [token for token in tokens if len(token) > 3]
    stemmed_tokens = [stemmer.stem(token) for token in tokens]
    return ' '.join(stemmed_tokens)

# Function to scrape and process new reviews
def scrape_and_process_reviews(collection):
    # Scrape new reviews from the last scraped date to today
    last_scraped_date_record = collection.find_one(sort=[("at", -1)])  # Assuming 'at' is the timestamp field
    if last_scraped_date_record:
        last_scraped_date_str = last_scraped_date_record['at']
        if isinstance(last_scraped_date_str, str):
            last_scraped_date = datetime.strptime(last_scraped_date_str, "%Y-%m-%d %H:%M:%S")
        else:
            last_scraped_date = last_scraped_date_str
    else:
        last_scraped_date = datetime.now() - timedelta(days=30)
    print(f'Last  date: {last_scraped_date}')
    
    result, _ = reviews('id.co.bri.brimo', count=10000, lang="id", country="id", sort=Sort.NEWEST, filter_score_with=None)
    
    new_reviews = [review for review in tqdm(result, desc="Filtering reviews") if review['at'] > last_scraped_date]
    scraped_count = len(new_reviews)
    print(f'Scraped {scraped_count} new reviews.')
    if not new_reviews:
        return
    # Preprocess reviews
    new_reviews_text = [preprocess_text(review['content']) for review in tqdm(new_reviews, desc="Preprocessing reviews")]

    # Predict sentiment labels
    predictions = pipeline.predict(new_reviews_text)

    # Assign topic to each review
    for i, review in enumerate(tqdm(new_reviews, desc="Assigning topics")):
        tfidf_matrix = pipeline.named_steps['tfidf'].transform([review['content']])
        if predictions[i] == 0:  # negative sentiment
            doc_topic_distributions = lda_model_0.transform(tfidf_matrix)
        else:  # positive sentiment
            doc_topic_distributions = lda_model_1.transform(tfidf_matrix)
        topic_assignments = doc_topic_distributions.argmax(axis=1)

        #merge data
        review['label'] = predictions[i]
        review['topic'] = topic_assignments[0]

    # If there are new reviews, add them to the MongoDB collection
    if new_reviews:
        new_reviews = pd.DataFrame(new_reviews)
        
        # Keep only the desired columns
        new_reviews = new_reviews[['reviewId', 'userName', 'userImage', 'content', 'score', 'at', 'label', 'topic']]
        
        # Convert the 'at' column to string format
        new_reviews['at'] = new_reviews['at'].astype(str)
        
        #save to csv
        new_reviews.to_csv('reviews.csv', index=False)
        # Insert to collection
        collection.insert_many(new_reviews.to_dict('records'))
        print(f'Inserted {len(new_reviews)} new reviews.')

if __name__ == "__main__":
    uri = "mongodb+srv://fajarkrisnajaya:jayanti890@cluster0.4oq7clz.mongodb.net/?retryWrites=true&w=majority"
    db_name = 'brimo_reviews'
    collection_name = 'user_reviews'

    stop_words = set(stopwords.words('indonesian'))
    factory = StemmerFactory()
    stemmer = factory.create_stemmer()
    
    # Load models
    pipeline = joblib.load('static/models/svc_classifier.joblib')
    lda_model_0 = joblib.load('static/models/lda_label0.joblib')  # Assuming both sentiment groups use the same LDA model
    lda_model_1 = joblib.load('static/models/lda_label1.joblib')  # Assuming both sentiment groups use the same LDA model

    # Establish MongoDB connection
    collection = connect_to_mongodb(uri, db_name, collection_name)

    # Scrape and process reviews
    scrape_and_process_reviews(collection)
