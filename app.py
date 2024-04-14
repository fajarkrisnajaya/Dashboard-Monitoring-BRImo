import json
import os
from flask import Flask, render_template, request, jsonify
from flask_caching import Cache
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
import plotly.io as pio
import plotly.offline as py
import plotly.graph_objs as go
import plotly
from google_play_scraper import app as scaper_app
from datetime import datetime, timedelta
app = Flask(__name__, template_folder="templates")
cache = Cache(app, config={'CACHE_TYPE': 'simple'})

uri = "mongodb+srv://fajarkrisnajaya:jayanti890@cluster0.4oq7clz.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri)
db = client['brimo_reviews']
collection = db['user_reviews']

import json
import plotly
#####################################Data Fetching and Visualization#######################################################
negative_topics = {
    0: "Internet dan Aplikasi",
    1: "Performa dan Login",
    2: "Gangguan Verifikasi",
    3: "Transaksi dan Layanan Perbankan",
    4: "Akses Akun",
    5: "Manajemen Kartu dan E-banking",
    6: "Bugs Aplikasi",
    7: "Masalah Pengunduhan dan Server",
    8: "Masalah Saldo dan Pembayaran",
    9: "Kesulitan Registrasi"
}

positive_topics = {
    0: "Manfaat",
    1: "Kecepatan",
    2: "Kemudahan",
    3: "Efisiensi",
    4: "Kepuasan",
    5: "Terima Kasih",
    6: "Apresiasi Aplikasi",
    7: "Ucapan Syukur",
    8: "Kepraktisan"
}
pipeline_sunburst = [
    {'$group': {'_id': {'label': '$label'}, 'count': {'$sum': 1}}},
    {'$project': {'_id': 0, 'label': '$_id.label', 'count': 1}},
    {'$sort': {'count': 1}}
]

@cache.memoize(timeout=5000)
def fetch_data(pipeline):
    result = list(collection.aggregate(pipeline))
    df = pd.DataFrame(result)
    return df

def generate_line_chart(df):
    df = df.sort_values('at')
    df['label'] = df['label'].replace({0: 'Negatif', 1: 'Positif'})
    df = df.rename(columns={'label': 'Sentimen'})

    fig = px.line(df, x='at', y='count', color='Sentimen')
    fig.update_layout(
        xaxis_title="Tanggal",
        yaxis_title="Frekuensi",
        coloraxis_colorbar_title="Sentimen"
    )
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON

def generate_sunburst_chart(df):
    df['label'] = df['label'].replace({1: 'Positif', 0: 'Negatif'})
    fig = px.pie(df, names='label', values='count')
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON

def generate_negative_topic_chart(df):
    fig = px.bar(df, x='at', y='count', color='topic', barmode='group', labels={'count': 'Frekuensi Ulasan', 'at': 'Tahun', 'topic': 'Topik Negatif'})
    fig.update_layout(xaxis={'type': 'category', 'categoryorder': 'category ascending'})  # Adjust the tickangle value as needed
    fig.update_traces(text=df['count'], textposition='outside')
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON

def generate_positive_topic_chart(df):
    fig = px.bar(df, x='at', y='count', color='topic', barmode='group', labels={'count': 'Frekuensi Ulasan', 'at': 'Tahun', 'topic': 'Topik Positif'})
    fig.update_layout(xaxis={'type': 'category', 'categoryorder': 'category ascending'})
    fig.update_traces(text=df['count'], textposition='outside')
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON
    

def get_reviews_details():
    total_documents = collection.count_documents({})
    latest_date = pd.to_datetime(collection.find_one(sort=[('at', -1)])['at'])


    # Get the app details from Google Play
    result = scaper_app(
        'id.co.bri.brimo',
        lang='id', # defaults to 'en'
        country='id' # defaults to 'us'
    )

    # Get the score and the number of installs
    score = str(round(result['score'], 2))
    installs = result['installs']

    return total_documents, latest_date, score, installs
######################################Scheduler#############################################################################
from apscheduler.schedulers.background import BackgroundScheduler
import subprocess
def schedule_scraper():
    # Create a scheduler
    scheduler = BackgroundScheduler()
    
    # Define the time interval or specific times when you want to run the scraper
    # For example, to run it daily at 2:00 AM:
    scheduler.add_job(run_scraper_script, 'cron', hour=0, minute=30, second=0)
    
    # Start the scheduler
    scheduler.start()

import threading

scraper_lock = threading.Lock()

def run_scraper_script():
    global scraper_lock
    with scraper_lock:
        # Run the scraper.py script as a separate process
        subprocess.run(["python", "scraper.py"])

# Call the function to schedule the scraper
schedule_scraper()
#############################################################################################################################
@app.route('/api/users', methods=['GET'])
def get_users():
    page = request.args.get('page', 1, type=int)
    per_page = 10
    filter_date = request.args.get('date', None)

    query = {}
    if filter_date:
        # Use a regular expression to match the date part of the string
        query["at"] = {
            "$regex": "^" + filter_date
        }

    users_cursor = collection.find(query).sort('at', -1).skip((page - 1) * per_page).limit(per_page)
    total = collection.count_documents(query)
    users = list(users_cursor)

    # Map the "label" directly to "Positif" or "Negatif" and map the "topic"
    for user in users:
        user['_id'] = str(user['_id']) 
        user['label'] = 'Positif' if user['label'] == 1 else 'Negatif'
        # Use get() method with a default value to handle KeyError
        user['topic'] = negative_topics.get(user['topic'], 'Unknown') if user['label'] == "Negatif" else positive_topics.get(user['topic'], 'Unknown')

    # Assuming users can be directly serialized; if not, you'll need to convert them
    return jsonify({'users': users, 'total': total, 'page': page, 'per_page': per_page})



@app.route("/")
def index():
    #########################################MONGODB PIPELINE###############################################################
    pipeline_line = [
        {'$set': {'at': {'$dateToString': {'date': {'$toDate': '$at'}, 'format': '%Y-%m-%d'}}}},
        {'$group': {'_id': {'at': '$at', 'label': '$label'}, 'count': {'$sum': 1}}},
        {'$project': {'_id': 0, 'at': '$_id.at', 'label': '$_id.label', 'count': 1}}
    ]

    pipeline_sunburst = [
        {'$group': {'_id': {'label': '$label', 'score': '$score'}, 'count': {'$sum': 1}}},
        {'$project': {'_id': 0, 'label': '$_id.label', 'score': '$_id.score', 'count': 1}},
        {'$sort': {'score': 1}}
    ]

    pipeline_topic_label1 = [
    {'$match': {'label': 1}},
    {'$set': {'at': {'$dateToString': {'date': {'$toDate': '$at'}, 'format': '%Y'}}}},
    {'$group': {'_id': {'at': '$at', 'topic': '$topic'}, 'count': {'$sum': 1}}},
    {'$project': {'_id': 0, 'at': '$_id.at', 'topic': '$_id.topic', 'count': 1}}
    ]

    pipeline_topic_label0 = [
    {'$match': {'label': 0}},
    {'$set': {'at': {'$dateToString': {'date': {'$toDate': '$at'}, 'format': '%Y'}}}},
    {'$group': {'_id': {'at': '$at', 'topic': '$topic'}, 'count': {'$sum': 1}}},
    {'$project': {'_id': 0, 'at': '$_id.at', 'topic': '$_id.topic', 'count': 1}}
    ]
    ###########################################GRAPH INITIALIZATION#########################################################
    df_line = fetch_data(pipeline_line)
    graphJSON_line = generate_line_chart(df_line)

    df_sunburst = fetch_data(pipeline_sunburst)
    graphJSON_sunburst = generate_sunburst_chart(df_sunburst)

    df_topic_label0 = fetch_data(pipeline_topic_label0)
    df_topic_label0['topic'] = df_topic_label0['topic'].map(negative_topics)
    graphJSON_topic_label0 = generate_negative_topic_chart(df_topic_label0)

    df_topic_label1 = fetch_data(pipeline_topic_label1)
    df_topic_label1['topic'] = df_topic_label1['topic'].map(positive_topics)
    graphJSON_topic_label1 = generate_positive_topic_chart(df_topic_label1)


    ########################################################################################################################
    total_documents, latest_date, score, installs= get_reviews_details()


    return render_template("index.html", graphJSON_line=graphJSON_line, graphJSON_sunburst=graphJSON_sunburst, total_documents=
                           total_documents,latest_date=latest_date, score=score, installs=installs,graphJSON_topic_label0=graphJSON_topic_label0,
                           graphJSON_topic_label1=graphJSON_topic_label1)
#############################################################################################################################
@app.route('/update_data_pie/<string:time_range>', methods=['GET'])
def update_data_pie(time_range):
    pipeline_sun = list(pipeline_sunburst)
    # Determine the start date based on the time range
    if time_range == '3M':
        start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d %H:%M:%S')
    elif time_range == '3D':
        start_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')
    elif time_range == '7D':
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    elif time_range == '1M':
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
    else:  # 'MAX'
        start_date = None

    # Modify the pipeline_sunburst to filter the data based on the start date
    if start_date is not None:
        pipeline_sun.insert(0, {'$match': {'at': {'$gte': start_date}, 'label': {'$exists': True}}})
  
    # Fetch and process the data for the sunburst chart
    df_sunburst = fetch_data(pipeline_sun)
    print(df_sunburst)
    graphJSON_sunburst = generate_sunburst_chart(df_sunburst)

    # Return the updated data
    return jsonify({'graphJSON_sunburst': graphJSON_sunburst})


@app.route("/documentation")
def documentation():
    return render_template("documentation.html")
    
@app.route('/health')
def health_check():
    return 'OK', 200

@app.route('/update', methods=['POST'])
def update_data():
    run_scraper_script()
    return jsonify({'status': 'success'}), 200


if __name__ == '__main__':
    app.run('')
