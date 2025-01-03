import json

def get_sql_server_config():
    try:
        with open('config\ssms.json') as fp:
            cred = json.load(fp)
    except Exception as e:
        print(e)
    return cred

def get_gmail_config():
    try:
        with open('config\gmail.json') as fp:
            gmail_credential = json.load(fp)

    except Exception as e:
        print(e)
    return gmail_credential

