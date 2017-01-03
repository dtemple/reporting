
### INTERNAL DASHBOARD ###
#TODO allow the user to adjust the number of messages required to count as active
#TODO allow the user to select dates and create the front report on the fly
#TODO bring in parse data to show invoices created, etc.

### EXTERNAL REPORTING ###
#TODO create a page for each hotel using their hotel name and then allow them to filter the data just in there


import requests
import pandas as pd
import numpy as np
import json
import io
import datetime

from flask import Flask
from flask import render_template
from flask import abort
app = Flask(__name__)

## Create a dict to match the tag to a hotel
# TODO Create this dynamically using the tags in the report
hoteltagsmap = {
    'Marcel': 'marcel-email',
    'Kabuki': 'kabuki-email',
    'Zeppelin': 'zeppelin-email',
    'Walker': 'walker-email',
    'Ameritania': 'ameritania-email',
    'Zetta': 'zetta-email',
    'GalleriaPark': 'galleriapark-email',
    'Zelos': 'zelos-email',
    'Triton': 'triton-email',
    'Gregory': 'gregory-email',
    'Presidio': 'presidio-email'
}

hotelinboxmap = {
    'NYC SMS: Marcel':'Marcel',
    'SF SMS: Kabuki':'Kabuki',
    'SF SMS: Zeppelin':'Zeppelin',
    'NYC SMS: Walker Hotel':'Walker',
    'NYC SMS: Ameritania':'Ameritania',
    'SF SMS: Zetta':'Zetta',
    'SF SMS: Galleria Park Hotel':'GalleriaPark',
    'SF SMS: Hotel Zelos':'Zelos',
    'SF SMS: Triton':'Triton',
    'NYC SMS: Gregory':'Gregory',
    'SF SMS: Inn at the Presidio':'Presidio'
}

def get_emails(data):
    # Remove the NaN values
    dfNoNa = data.dropna(subset=['tags'])
    # Just keep the email records
    dfemails = dfNoNa[dfNoNa['tags'].str.lower().str.contains("-email")]

    # Figure out which hotel each record is

    # Use the dict to create a dataframe
    emaildata = pd.DataFrame(list(hoteltagsmap.items()),
                             columns=['hotel-name', 'hotel-tag'])

    inboundlist = []
    outboundlist = []

    for hotel, tag in hoteltagsmap.items():
        # create a df with just the data for that hotel
        dftags = dfemails[dfemails['tags'].map(lambda tags: tag in tags.lower())]
        # assign the right data to a master df
        values = pd.value_counts(dftags['direction'].values, sort=False)
        inboundlist.append(values['Inbound'])
        outboundlist.append(values['Outbound'])

    emaildata['inbound_emails'] = inboundlist
    emaildata['outbound_emails'] = outboundlist
    emaildata['total_emails'] = emaildata['inbound_emails'] + emaildata['outbound_emails']

    # Drop the tag column
    emaildata=emaildata.drop('hotel-tag',1)

    # Sort the table
    emaildata.sort_values('total_emails', ascending=False, inplace=True)

    # Return a dataframe with the columns: index, hotels, hotel-tags, inboundemails, outboundemails, totalemails
    return emaildata

def prod_csv(x):
    url = x
    s = requests.get(url).content
    cols = ['message_id', 'conversation_id', 'segment', 'direction', 'status', 'inbox', 'msg_date', 'reaction_time',
            'resolution_time', 'resp_time', 'assignee',
            'author', 'contact_name', 'contact_handle', 'to', 'cc', 'bcc', 'extract', 'tags']
    # Upload the file
    df = pd.read_csv(io.StringIO(s.decode('utf-8')), names=cols, dtype={"message_id":np.int32, 'conversation_id':np.int32, 'segment':str, 'direction':str, 'status':str, 'inbox':str, 'msg_date':str, 'reaction_time':str,
        'resolution_time':str, 'resp_time':str, 'assignee':str,
        'author':str, 'contact_name':str, 'contact_handle':str, 'to':str, 'cc':str, 'bcc':str, 'extract':str, 'tags':str}, skiprows=1)
    # remove junk inboxes
    data = df.loc[~df['inbox'].isin(
        ['SD App', 'Vendors', 'Arrivals', '02 - Reservations', 'Support (Front desks)', '01 - Payments', 'Arrivals-dev',
         'SMS: Demo Hotel'])]
    return data

def testing_csv():
    cols = ['message_id', 'conversation_id', 'segment', 'direction', 'status', 'inbox', 'msg_date', 'reaction_time',
            'resolution_time', 'resp_time', 'assignee',
            'author', 'contact_name', 'contact_handle', 'to', 'cc', 'bcc', 'extract', 'tags']

    s = "/Users/dtemple/PycharmProjects/testing/front.csv"

    df = pd.read_csv(s, names=cols, dtype={"message_id":np.int32, 'conversation_id':np.int32, 'segment':str, 'direction':str, 'status':str, 'inbox':str, 'msg_date':str, 'reaction_time':str,
        'resolution_time':str, 'resp_time':str, 'assignee':str,
        'author':str, 'contact_name':str, 'contact_handle':str, 'to':str, 'cc':str, 'bcc':str, 'extract':str, 'tags':str}, skiprows=1)

    # remove junk inboxes
    data = df.loc[~df['inbox'].isin(
        ['SD App', 'Vendors', 'Arrivals', '02 - Reservations', 'Support (Front desks)', '01 - Payments', 'Arrivals-dev',
         'SMS: Demo Hotel'])]

    # remove useless columns
    data.drop(['segment', 'reaction_time', 'resolution_time', 'resp_time', 'assignee', 'cc', 'bcc', 'extract'], 1,
              inplace=True)

    return data

def get_inbox_table(data):

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
    # TODO clean up everything above
    master = pd.merge(master, guest, left_index=True, right_index=True)
    master.sort_values('active_guests', ascending=False, inplace=True)

    # Add a column with the hotel names
    #for x in master['inbox']:
        #hotel


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
            # for testing
            # data=testing_csv()
            # prod get front csv
            data = prod_csv(row['url'])
            object_list=get_inbox_table(data)
            emaildata=get_emails(data)
            return render_template(template, tables=[object_list.to_html(classes='bluestyle')], emails=[emaildata.to_html(classes='bluestyle')], titles=['Inbox Data'])
    abort(404)

if __name__ == '__main__':
    # Fire up the Flask test server
    app.run(debug=True, use_reloader=True)
