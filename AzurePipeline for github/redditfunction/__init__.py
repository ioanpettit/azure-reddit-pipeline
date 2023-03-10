import datetime
import logging
import pandas as pd
import pyodbc
import requests as re
import numpy as np
import requests.auth
from time import sleep
import datetime
import logging
import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
 #--------------------------------------------------------------------
    
    # function to get the authorisation code from reddit
    def auth(client_id, client_secret):
        client_auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
        headers = {"User-Agent": "PostmanRuntime/7.31.1",
                "Content-Type": "application/x-www-form-urlencoded"}
        body = {"grant_type": "client_credentials"}
        r = re.post("https://www.reddit.com/api/v1/access_token",
                    data=body, auth=client_auth, headers=headers).json()
        token = r["access_token"]
        sleep(2)
        return token

    # function to get the new submissions for a certain subreddit
    def NewSubmissions(subreddit, iterations, limit, token, after=None):
        i = 0
        df = pd.DataFrame()
        while i < iterations:
            if after is None:
                pickup = ""
            else:
                pickup = "t3_" + str(after)
            headers = {"User-Agent": "PostmanRuntime/7.31.1",
                    "Authorization": f"Bearer {token}"}
            params = {"limit": f"{limit}",
                    "after": f"{pickup}"}
            r = re.get(f"https://oauth.reddit.com/r/{subreddit}/new.json",
                    headers=headers,params=params)
            r = r.json()["data"]["children"]
            for key, value in enumerate(r):
                r[key] = r[key]["data"]
            df = pd.concat((df,pd.DataFrame(r)),ignore_index=True)
            i+=1
            sleep(2)
            after = df["id"][len(df["id"])-1]
        df["created"]=(pd.to_datetime(df["created"],unit = 's'))
        return df[["id","title","ups", "upvote_ratio","gilded","created","subreddit"]]

    #add reddit tokens here and this will get the token
    token = auth("",
                "")

    # using NewSubmissions() function to get new posts
    response1 = NewSubmissions("teslamotors", 10, 100, token)
    response2 = NewSubmissions("amazon", 10, 100, token)

    # function to send the posts to an azure database
    def to_azure_sql(dataframe):
        server = 'tcp:' #add server here
        database = '' # add database name
        username = '' # add username
        password = '{}' # add password
        driver = '{ODBC Driver 17 for SQL Server}'
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)

        #starting cursor instance
        cursor = conn.cursor()
        #inserting posts
        for index, row in dataframe.iterrows():
            cursor.execute("""IF NOT EXISTS (SELECT * FROM dbo.reddit_table WHERE id = ?)
                BEGIN
                INSERT INTO dbo.reddit_table (id,title,ups,upvote_ratio,gilded,created,subreddit) VALUES(?,?,?,?,?,?,?)
                END""", str(row.id), str(row.id), str(row.title), str(row.ups), str(row.upvote_ratio), str(row.gilded), str(row.created),str(row.subreddit))
        
        conn.commit()
        cursor.close()
    # running the function
    to_azure_sql(response1)
    to_azure_sql(response2)


 #--------------------------------------------------------------------
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
