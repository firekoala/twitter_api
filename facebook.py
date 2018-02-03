#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import requests
import calendar
import time
import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd

MONTH = '2018-01'
FBTOKEN = 'EAACEdEose0cBAGxK9upZAhG2HCdRG9xzpuSWY8cZAVTfmA6ZCPTZAweYw1BElZCmoAyBFVLdl0IMCJGsBkzAxk3tsVdultgPiqTjkgBpZBZApRNrCSL5jExwH1KiWlKubrZBoSkSTsQjwoJK8POTShdJq1ZCy2mQoOKAtOmq6qciVrSnaZAZCiZB1xpI7dv5aeoEXKgZD'

def get_data_all_clients():
    with open('clients.json') as clients_file:
        clients = json.load(clients_file)
    for client in clients:
        if 'facebook_id' not in client: continue
        get_data(client)
        if 'facebook2_id' in client: get_data({'name':client['name'],'facebook_id':client['facebook2_id']}, second_page=True)

def get_data(client,second_page=False):
# r = requests.get('https://graph.facebook.com/v2.8/{}/insights?access_token={}&pretty=0&since={}&until={}&metric=page_actions_post_reactions_total,page_posts_impressions,page_posts_impressions,page_posts_impressions_unique,page_fans,page_fan_adds&period=day'.format(
    print 'collecting insights metrics for {}...'.format(client['name'])

    year = int(MONTH.split('-')[0])
    month = int(MONTH.split('-')[1])
    previous_month = 12 if (month == 1) else month - 1
    currentMonth = dt.date(year, month, 1)
    until = int(time.mktime(currentMonth.timetuple()))
    previousYear = (dt.date(year -1,month,1) - dt.timedelta(days=1)).replace(day=1)
    since = int(time.mktime(previousYear.timetuple()))

    maxInterval = 8035200
    lowerbound = since
    upperbound = since + maxInterval
    allResults = []
    while lowerbound < until:
        try:
            r = requests.get('https://graph.facebook.com/v2.8/{}/insights?access_token={}&pretty=0&since={}&until={}&metric=page_actions_post_reactions_total,page_posts_impressions,page_posts_impressions_unique,page_engaged_users,page_posts_impressions_paid,page_posts_impressions_paid_unique,page_posts_impressions_organic_unique,page_fans,page_fan_adds&period=day'.format(client['facebook_id'], FBTOKEN, lowerbound, min(upperbound,until)))
            r2 = requests.get('https://graph.facebook.com/v2.8/{}/insights?access_token={}&pretty=0&since={}&until={}&metric=page_fans'.format(client['facebook_id'], FBTOKEN, lowerbound, min(upperbound,until)))

            lowerbound += maxInterval
            upperbound = lowerbound +maxInterval
            allResults.append(r.json()["data"]+r2.json()['data'])
        except:
            print 'failed {}'.format(upperbound)

    metrics = [metric['name'] for metric in allResults[0]]
    # dates = [row['end_time'][:10] for row in allResults[0][0]["values"]]
    data_raw = {}
    for metric in metrics :
        data_raw[metric] = []
    for res in allResults:
        for metric in res:
            data_raw[metric['name']]+=metric['values']

    dates_raw = [row['end_time'] for row in data_raw[metrics[0]]]
    data = []
    for i in xrange(len(dates_raw)):
        date = dates_raw[i]
        row = {'date': date[:10]}
        for metric in metrics:
            row[metric] = data_raw[metric][i]['value'] if 'value' in data_raw[metric][i] else 0
        data.append(row)

    df = pd.DataFrame(data)
    def sum_dict(d):
        s = 0
        for v in d.itervalues():
            s+=v
        return s
    df['page_reactions'] = df['page_actions_post_reactions_total'].apply(sum_dict)
    del df['page_actions_post_reactions_total']
    df['date'] = pd.to_datetime(df.date,format=("%Y-%m-%d"))

    keep_last= lambda x: x[-1]
    df = df.set_index('date')
    monthly = df.resample('M').sum().reset_index()
    monthly['date'] = monthly['date'].apply(lambda x: x.strftime("%Y-%m-%d"))
    monthly = monthly.set_index('date')
    monthly['page_posts_impressions_organic'] = monthly['page_posts_impressions'] - monthly['page_posts_impressions_paid']
    monthly['page_posts_impressions_organic_unique'] = monthly['page_posts_impressions_unique'] - monthly['page_posts_impressions_paid_unique']
    page_fans = df.resample('M').apply(keep_last)['page_fans']

    def df_to_json(df):
        data = []
        df = df.reset_index()
        cols = df.columns
        for row in df.iterrows():
            line = {}
            for col in cols:
                line[col] = row[1][col]
            data.append(line)
        return data

    def series_to_json(s):
        data = []
        for index,value in s.iteritems():
            data.append({'date': index, 'value': value})
        return data

    facebook_data = {'kpi': {}}
    facebook_data['other_kpi'] = {}
    facebook_data['recrutement'] = series_to_json(monthly['page_fan_adds'])
    facebook_data['reach_paid'] = series_to_json(monthly['page_posts_impressions_paid_unique'])
    facebook_data['reach_organic'] = series_to_json(monthly['page_posts_impressions_organic_unique'])
    facebook_data['impressions_paid'] = series_to_json(monthly['page_posts_impressions_paid'])
    facebook_data['impressions_organic'] = series_to_json(monthly['page_posts_impressions_organic'])
    facebook_data['reach'] = df_to_json(monthly[['page_posts_impressions_paid_unique','page_posts_impressions_organic_unique','page_posts_impressions_paid','page_posts_impressions_organic']])


    reach_current = float(monthly['page_posts_impressions_organic_unique'].iloc[-1])
    reach_previous = float(monthly['page_posts_impressions_organic_unique'].iloc[-2])
    reach_all_current = float(monthly['page_posts_impressions_organic_unique'].iloc[-1]+monthly['page_posts_impressions_paid_unique'].iloc[-1])
    reach_all_previous = float(monthly['page_posts_impressions_organic_unique'].iloc[-2]+monthly['page_posts_impressions_paid_unique'].iloc[-2])

    # engagement_current = float(monthly['page_engaged_users'].iloc[-1])
    # engagement_previous = max(float(monthly['page_engaged_users'].iloc[-2]),1)
    impressions_current = float(monthly['page_posts_impressions_organic'].iloc[-1])
    impressions_previous = float(monthly['page_posts_impressions_organic'].iloc[-2])
    impressions_all_current = float(monthly['page_posts_impressions_organic'].iloc[-1])+float(monthly['page_posts_impressions_paid'].iloc[-1])
    impressions_all_previous = float(monthly['page_posts_impressions_organic'].iloc[-2])+float(monthly['page_posts_impressions_paid'].iloc[-2])

    followers_current = float(int(page_fans.iloc[-1]))
    followers_previous = float(int(page_fans.iloc[-2]))

    # facebook_data['other_kpi']['engagement'] = {
    #     'title': 'Engagement',
    #     'value': engagement_current,
    #     'increase': engagement_current/engagement_previous - 1
    # }
    def increase(a,b):
        try:
            return (a/b) -1
        except:
            return "NA"
    facebook_data['other_kpi']['reach_organic'] = {
        'title': 'Portée',
        'value': reach_current,
        'increase': increase(reach_current,reach_previous)
    }
    facebook_data['kpi']['reach'] = {
        'title': 'Portée',
        'value': reach_all_current,
        'increase': increase(reach_all_current,reach_all_previous)
    }
    facebook_data['other_kpi']['impressions_organic'] = {
        'title': 'Impressions',
        'value': impressions_current,
        'increase': increase(impressions_current,impressions_previous)
    }
    facebook_data['kpi']['impressions'] = {
        'title': 'Impressions',
        'value': impressions_all_current,
        'increase': increase(impressions_all_current,impressions_all_previous)
    }
    facebook_data['kpi']['followers'] = {
        'title': 'Nombre de fans',
        'value': followers_current,
        'increase': increase(followers_current,followers_previous)
    }

    # fetch all posts past 2 month
    previousMonth = currentMonth - relativedelta(months=2)
    since = int(time.mktime(previousMonth.timetuple()))
    print 'collecting post data for {}...'.format(client['name'])

    r = requests.get('https://graph.facebook.com/v2.8/{}/posts?limit=100&access_token={}&since={}&until={}'.format(client['facebook_id'],FBTOKEN, since, until))
    post_ids = [post['id'] for post in r.json()['data']]

    posts = []
    for post_id in post_ids:
        r = requests.get('https://graph.facebook.com/v2.8/{}?access_token={}&fields=shares,comments,likes,picture,message,created_time'.format(post_id,FBTOKEN))
        raw = r.json()
        p = {}
        p['shares'] = raw['shares']['count'] if 'shares' in raw else 0
        p['date'] = raw['created_time'][:10]
        p['fulldate'] = raw['created_time']
        p['message'] = raw['message']  if 'message' in raw else 'No message'
        p['image'] = raw['picture'] if 'picture' in raw else ''
        reactions = requests.get('https://graph.facebook.com/v2.8/{}/reactions?access_token={}&summary=true'.format(post_id,FBTOKEN))
        p['likes']=reactions.json()['summary']['total_count']
        comments = requests.get('https://graph.facebook.com/v2.8/{}/comments?access_token={}&summary=true'.format(post_id,FBTOKEN))
        p['comments'] = comments.json()['summary']['total_count']
        posts.append(p)

    posts_df = pd.DataFrame(posts)
    engagement = posts_df.groupby('date').sum()


    beginning = (currentMonth - relativedelta(months=1)).strftime("%Y-%m-%d")
    facebook_data['engagement'] = df_to_json(engagement)
    facebook_data['posts'] = [post for post in posts if post['date'] >= beginning]

    engagement_current = float(engagement[engagement.index >= beginning].sum().sum())
    a = engagement[engagement.index < beginning]
    a = a[a.index >= previousMonth.strftime('%Y-%m-%d')]
    engagement_previous = max(1,float(a.sum().sum()))

    facebook_data['kpi']['engagement'] = {
        'title': 'Engagement',
        'value': engagement_current,
        'increase': engagement_current/engagement_previous - 1
    }

    if second_page :
        with open('dist/'+client['name']+'/facebook_data2.json', 'wb') as outfile:
            json.dump(facebook_data, outfile)
    else :
        with open('dist/'+client['name']+'/facebook_data.json', 'wb') as outfile:
            json.dump(facebook_data, outfile)
    return facebook_data

if __name__ == "__main__":
    get_data_all_clients()
    # try :
    #     get_data_all_clients()
    # except :
    #     MONTH = raw_input('MONTH :')
    #     FBTOKEN = raw_input('TOKEN :')
    #     get_data_all_clients()
