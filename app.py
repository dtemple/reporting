
### INTERNAL DASHBOARD ###
#TODO Test and deploy
#TODO Add a column to the pandas df to include email counts in total messages
## Do that by parsing all the email tags for the name of the property and then adding to that inbox
#TODO allow the user to adjust the number of messages required to count as active
#TODO allow the user to select dates and create the front report on the fly
#TODO bring in parse data to show invoices created, etc.

### EXTERNAL REPORTING ###
#TODO create a page for each hotel using their hotel name and then allow them to filter the data just in there


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


# Function to get the exports from Front
def get_exports():
    payload = {'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzY29wZXMiOlsiKiJdLCJpc3MiOiJmcm9udCIsInN1YiI6InNjb3V0In0.t6HbvRE_UCHJeBYLcVvXwasd5rH9EELCfDevw9fsvpw',
               'Accept': 'application/json'}
    url='https://api2.frontapp.com/exports?page=1'
    t = requests.get(url, headers=payload)
    jsondict = t.json()
    return jsondict

# HERE IS HOW TO PRINT THE ITEMS IN ThE LIST
#for num,obj in masterdict.items():
#    print('status: %r, query_start: %r, created at: %r' % (obj['status'],obj['query_start'],obj['created_at']))


# Take the front exports, get rid of the noise and put them in a readable dict of dicts
# Another option here is to create a pd.dataframe from this data
def get_results(jsondict):
    resultslist=jsondict['_results']
    i=0
    masterdict={}
    x={}
    for i in range(0, len(resultslist)):
        x['id'] = resultslist[i]['id']
        x['url'] = resultslist[i]['url']
        x['status'] = resultslist[i]['status']
        x['created_at'] = datetime.datetime.fromtimestamp(int(resultslist[i]['created_at'])).strftime('%m/%d/%Y %H:%M:%S')
        x['query_start'] = datetime.datetime.fromtimestamp(int(resultslist[i]['query']['start'])).strftime('%m/%d/%Y')
        x['query_end'] = datetime.datetime.fromtimestamp(int(resultslist[i]['query']['end'])).strftime('%m/%d/%Y')
        masterdict[i]=x
        x={}
        i=i+1
    return masterdict

@app.route("/")
def index():
    template = 'index.html'
    jsondict = get_exports()
    results_objects = get_results(jsondict)
    return render_template(template, results_objects=results_objects)

@app.route('/<row_id>/')
def detail(row_id):
    template = 'detail.html'
    jsondict = get_exports()
    results_objects = get_results(jsondict)
    for num, row in results_objects.items():
        if row['id'] == row_id:
            object_list=get_csv(row['url'])
            return render_template(template, tables=[object_list.to_html(classes='bluestyle')], titles=['Data'])
    abort(404)

if __name__ == '__main__':
    # Fire up the Flask test server
    app.run(debug=True, use_reloader=True)


    # NOT USED
    # def get_single_export(x):
    #     payload = {'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzY29wZXMiOlsiKiJdLCJpc3MiOiJmcm9udCIsInN1YiI6InNjb3V0In0.t6HbvRE_UCHJeBYLcVvXwasd5rH9EELCfDevw9fsvpw',
    #                'Accept': 'application/json'}
    #     response = requests.get('https://api2.frontapp.com/exports/'+ x, headers=payload)
    #     jsonData = response.text
    #     return jsonData

    # def get_query_dates(resultsJson):
    #     for x in resultsJson:
    #         print('ID is {}',x['id'])
    #         for y in x['query']:
    #             print(y['start'])
    #
    #         for key, value in resultsJson.items():
    #             if 'query' in value:
    #                 for start, dt in value['query'].items():
    #                     print(start, dt)
    # # for key, value in x.items():
    #     if 'query' in value:
    #         for start in value['query'].items():
    #             start['start']='123'
    #

    # def add_dates(full_export, export_id):
    #     payload = {'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzY29wZXMiOlsiKiJdLCJpc3MiOiJmcm9udCIsInN1YiI6InNjb3V0In0.t6HbvRE_UCHJeBYLcVvXwasd5rH9EELCfDevw9fsvpw',
    #                'Accept': 'application/json'}
    #     response = requests.get('https://api2.frontapp.com/exports/'+ export_id, headers=payload)
    #     jsonData = response.text
    #     x = json.loads(jsonData)
    #     exportstats = x['query']
    #     exportstats['start'] = datetime.datetime.fromtimestamp(int(exportstats['start'])).strftime('%Y-%m-%d %H:%M:%S')
    #     exportstats['end'] = datetime.datetime.fromtimestamp(int(exportstats['end'])).strftime('%Y-%m-%d %H:%M:%S')
    #     for export_id in full_export:
    #         full_export[startdate]=exportstats['start']
    #         full_export[enddate]=exportstats['end']
    #     return resultsJson


    # # results2 is saved as the json result of get exports from front
# #This function gets the start and end time of the export
# def get_start_end(results):
#     y = 0
#     for y in range(0, len(results)):
#         print('the %r start is %r' % (y, datetime.datetime.fromtimestamp(int(results[y]['query']['start'])).strftime('%Y-%m-%d %H:%M:%S')))
#         print('the %r end is %r' % (
#         y, datetime.datetime.fromtimestamp(int(results[y]['query']['end'])).strftime('%Y-%m-%d %H:%M:%S')))
#
#
#
#     for x in resultsJson:
#         x['created_at'] = datetime.datetime.fromtimestamp(int(x['created_at'])).strftime('%Y-%m-%d %H:%M:%S')
#     for x in resultsJson:
#         export_id=x['id']
#         add_dates(resultsJson, export_id)
#     return resultsJson

#def get_query(jsonData):
#    x = json.loads(jsonData)
#    queryJson=x['_results']['query']
#    return queryJson

#create resultsdict instead of resultslist
# def results_dict_create(jsondict):
#     for links, result in jsondict.items():