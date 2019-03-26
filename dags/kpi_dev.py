from __future__ import print_function
import pickle
import os.path
import requests
import math
import pandas as pd
import re
import datetime
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# If modifying these scopes, delete the file token.pickle.
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets'
]

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1Cyu1hKwjRFwp-GMkY0FDuTw0zQ1z4896XD1s9EBHlYo'

board_id = '/board/31'
issues_url = url_base + board_id + '/issue'
url_base = 'http://tsrs-jira.telessauders.ufrgs.br/rest/agile/1.0'
git_url = 'http://tsrs-git.telessauders.ufrgs.br/rest/api/1.0/projects/TS/repos/telessaude/'
board_url = url_base + board_id + '/sprint'
auth = ('eduardo.machado', 'dudu1903')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 3, 26),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@weekly',
}

dag = DAG("KPI's dev", catchup=False, default_args=default_args)

def extraction_jira():
    sprint = requests.request(
        url=board_url,
        auth=auth,
        method='GET',
        params={'state': u'active'}
    ).json()['values']
    sprint_issues = requests.request(
        url=url_base + '/sprint/' + str(sprint[0]['id'])+'/issue',
        auth=auth,
        method='GET',
        params={'fields': u'id'}
    ).json()['issues']
    sprint_issues = map(lambda x: x['id'], sprint_issues)
    issues = []
    response = requests.request(url=issues_url, auth=auth, method='GET')
    issues_count = response.json()['total']
    max_result = response.json()['maxResults']
    requests_left = math.ceil((issues_count - max_result - 0.0) / max_result)
    issues = issues + response.json()['issues']
    for index in range(int(requests_left)):
        response = requests.request(
            url=issues_url,
            auth=auth,
            method='GET',
            params={'startAt': index * max_result + max_results})
        issues = issues + response.json()['issues']
    board_url = url_base + board_id +'/version'
    versions = requests.request(url=board_url, auth=auth, method='GET').json()
    v = {}
    for x in versions['values']:
        v[x['id']] = x['releaseDate']
    return {'issues': issues}


def extraction_git():
    response = requests.request(url=git_url+'pull-requests', auth=auth, method='GET', params={'state': 'MERGED'}).json()
    prs = response['values']
    while not response['isLastPage']:
        response = requests.request(
            url=git_url+'pull-requests',
            auth=auth, 
            method='GET',
            params={
                'state': 'MERGED', 
                'start': len(prs)
            }
        ).json()
        prs = prs + response['values']
    issues_list = []
    def _reviewers(id):
	a = filter(lambda x: x['fromRef']['displayId'] == id, prs)
	if len(a):
	    map(lambda x: x['user']['name'], filter(lambda y: y['status'] == u'APPROVED', a[0]['reviewers']))
	    issue_url = url_base + '/issue/' + str(id)
	    response = requests.request(url=issue_url, auth=auth, method='GET')
	    return response.json()['fields']['assignee']['key']
	return ''

    def get_reviewers(x):
	cause = filter(lambda y: y['type']['inward'] == 'is caused by',x['fields'][u'issuelinks'])
	if len(cause):
	    return _reviewers(cause[0][u'inwardIssue']['key']) if u'inwardIssue' in cause[0] else ''
	return ''
    def get_responsavel(id):
	issues_url = url_base + '/issue/' + str(id)
	response = requests.request(url=issues_url, auth=auth, method='GET')
	return response.json()['fields']['assignee']['key']
    def get_links(x):
	cause = filter(lambda y: y['type']['inward'] == 'is caused by',x['fields'][u'issuelinks'])
	if len(cause):
	    return get_responsavel(cause[0][u'inwardIssue']['id']) if u'inwardIssue' in cause[0] else ''
	return ''
    return {'issues_list': issues_list}

    for x in kwargs.get('ti').xcom_pull(task_ids='extraction_jira').issues:
	issues_list.append({
	    'responsavel': x['fields']['assignee']['key'] if x['fields']['assignee'] else '',
	    'sprints': [s['name'] for s in x['fields']['closedSprints']] if 'closedSprints' in x['fields'] else [],
	    'sprint': re.search(r'Sprint \d+', sorted([(s['name'], s['id']) for s in x['fields']['closedSprints']], key=lambda x: x[1])[-1][0]).group(0) if 'closedSprints' in x['fields'] else '',
	    'criado': datetime.datetime.strptime(x['fields']['created'][:-9], "%Y-%m-%dT%H:%M:%S"),
	    'versao': datetime.datetime.strptime(v[int(x['fields']['fixVersions'][0]['id'])][:-10], "%Y-%m-%dT%H:%M:%S") if len(x['fields']['fixVersions']) else '',
	    'tipo': x['fields']['issuetype']['name'],
	    'prioridade': x['fields']['priority']['name'],
	    'status': x['fields']['status']['name'],
	    'caused_by': get_links(x),
	    'approved_by': get_reviewers(x),
	    'id': x['id'],
	    '_key': x['key'],
	    'estimativa': x['fields']['customfield_10006']
	})

def sprint_points(ds, **kwargs):
    df_issues = pd.DataFrame(kwargs.get('ti').xcom_pull(task_ids='extraction_jira').issues_list)
    df_issues['sprint'] = df_issues['sprint'].apply(lambda x: int(x.split(' ')[1]) if x else '')
    df_issues = df_issues[(df_issues['sprint'] != 8) &(df_issues['sprint'] != 10) & (df_issues['sprint'] != 12) & (df_issues['sprint'] != 14)]
    sprint_points = df_issues[(df_issues['status'] == 'Done') & (df_issues['tipo'] != 'Sub-tarefa')].groupby('sprint')['estimativa'].sum()
    sprint_points.loc[-1, :]


def load_sheets(ds, **kwargs):
    input_range = 'sprint_points!A18:O'
    value_input_option = 'USER_ENTERED'
    value_range_body = {
        'range': input_range,
        'majorDimension': ('ROWS'),
        'values': [
            kwargs.get('ti').xcom_pull(task_ids='sprint_points')
        ]
    }
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    sheet = service.spreadsheets()
    result = sheet.values().update(
        spreadsheetId=SAMPLE_SPREADSHEET_ID,
        range=input_range,
        valueInputOption=value_input_option,
        body=value_range_body
    ).execute()

ext_jira = PythonOperator(
    task_id='extraction_jira',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

ext_git = PythonOperator(
    task_id='extraction_git',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

sprint_points = PythonOperator(
    task_id='sprint_points',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

load_sheets = PythonOperator(
    task_id='load_sheets',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)
ext_jira >> ext_git >> sprint_points >> load_sheets
