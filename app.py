import requests
import pandas as pd
import json
import io
import datetime

from flask import Flask
from flask import render_template
from flask import abort
app = Flask(__name__)

def get_csv(x):
    url = x
    s = requests.get(url).content
    cols = ['message_id', 'conversation_id', 'segment', 'direction', 'status', 'inbox', 'msg_date', 'reaction_time',
            'resolution_time', 'resp_time', 'assignee',
            'author', 'contact_name', 'contact_handle', 'to', 'cc', 'bcc', 'extract', 'tags']
    # Upload the file
    df = pd.read_csv(io.StringIO(s.decode('utf-8')), names=cols)
    # REMOVE JUNK INBOXES
    data = df.loc[~df['inbox'].isin(
        ['SD App', 'Vendors', 'Arrivals', '02 - Reservations', 'Support (Front desks)', '01 - Payments', 'Arrivals-dev',
         'SMS: Demo Hotel'])]

    # Create a table with count of unique users + count of unique messages
    master = data.pivot_table(values=['contact_handle', 'message_id'], index=['inbox'],
                              aggfunc=lambda x: len(x.unique()))
    master.columns = ['total_guests', 'total_messages']

    # Get the inbound messages and then find them by inbox
    inbound_messages = data.loc[data['direction'] == 'Inbound']
    inbound_messages = inbound_messages.pivot_table(values=['message_id'], index=['inbox'],
                                                    aggfunc=lambda x: len(x.unique()))
    inbound_messages.columns = ['inbound_messages']

    # Merge
    master = pd.merge(master, inbound_messages, left_index=True, right_index=True)

    # Pivot to get the count of messages per guest
    # Remove guests with 3 or less
    # Pivot again to get the count, then change the column name
    # NOTE: CHANGED FROM CONVERSATION ID TO CONTACT HANDLE HERE
    guestsByMessageCount = data.pivot_table(values=['message_id'], index=['inbox', 'contact_handle'],
                                            aggfunc=lambda x: len(x.unique()))
    active_only = guestsByMessageCount.loc[guestsByMessageCount['message_id'] > 3]
    active_only.reset_index(inplace=True)  # resets the index to make all data into columns
    active_count = active_only.pivot_table(values=['contact_handle'], index=['inbox'],
                                           aggfunc=lambda x: len(x.unique()))
    active_count.columns = ['active_guests']

    # Merge
    master = pd.merge(master, active_count, left_index=True, right_index=True)

    # Most active guest, remove dupes
    guestnames = data.pivot_table(values=['message_id'], index=['inbox', 'contact_name'],
                                  aggfunc=lambda x: len(x.unique()))
    guestnames.sort_values('message_id', ascending=False, inplace=True)
    guestnames.reset_index(inplace=True)
    guest = guestnames.groupby('inbox').first()
    guest.columns = ['most_active_guest', 'longest_thread']
    # guest.drop('index', axis=1, inplace=True)

    # Merge
    master = pd.merge(master, guest, left_index=True, right_index=True)
    master.sort_values('total_guests', ascending=False, inplace=True)

    #csv_file = open(csv_path, 'r')
    #csv_obj = csv.DictReader(csv_file)
    #csv_list = df
    return master

def get_exports():
    payload = {'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzY29wZXMiOlsiKiJdLCJpc3MiOiJmcm9udCIsInN1YiI6InNjb3V0In0.t6HbvRE_UCHJeBYLcVvXwasd5rH9EELCfDevw9fsvpw',
               'Accept': 'application/json'}
    response = requests.get('https://api2.frontapp.com/exports?page=1', headers=payload)
    jsonData = response.text
    return jsonData

# Front get exports list
def get_results(jsonData):
    x = json.loads(jsonData)
    resultsJson=x['_results']
    for x in resultsJson:
        x['created_at'] = datetime.datetime.fromtimestamp(int(x['created_at'])).strftime('%Y-%m-%d %H:%M:%S')
    return resultsJson



#def get_query(jsonData):
#    x = json.loads(jsonData)
#    queryJson=x['_results']['query']
#    return queryJson

# TODO Transform the created at date

@app.route("/")
def index():
    template = 'index.html'
    fullJson = get_exports()
    results_objects = get_results(fullJson)
    #query_objects = get_query(fullJson)
    return render_template(template, results_objects=results_objects)

@app.route('/<row_id>/')
def detail(row_id):
    template = 'detail.html'
    fullJson = get_exports()
    results_objects = get_results(fullJson)
    for row in results_objects:
        if row['id'] == row_id:
            object_list=get_csv(row['url'])
            return render_template(template, tables=[object_list.to_html(classes='bluestyle')], titles=['Data'])
    abort(404)

if __name__ == '__main__':
    # Fire up the Flask test server
    app.run(debug=True, use_reloader=True)