import requests

class Youtube():
    
    def __init__(self,client_id=None,client_secret=None,refresh_token=None):
        
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        
        if (self._client_id ==None) or (self._client_secret==None) or (self._refresh_token==None):
            
            print('Please provide all credentials')
            
            
        
    def accessToken(self,client_id, client_secret, refresh_token):
        params = {
                "grant_type": "refresh_token",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "refresh_token": self._refresh_token
        }


        authorization_url = "https://www.googleapis.com/oauth2/v4/token"

        response = requests.post(authorization_url, data=params)

        if response.status_code:
                return response.json()['access_token']
        else:
                return print(f"You've got response code : {response.status_code}")

    
    def YT_data_extraction(self,playlist_id):
    
        access_token = self.accessToken(client_id=self._client_id,client_secret=self._client_secret,refresh_token=self._refresh_token)

        print(f"access token is : {access_token}")

        url = 'https://www.googleapis.com/youtube/v3/playlistItems'

        param_ = {
            'access_token' : access_token,
            'part' : 'id,contentDetails',
            'playlistId' : playlist_id,
            'maxResults' : 150
            } 

        response = requests.get(url,params=param_).json()


        #Content Details

        videoId = list(map(lambda x: x['contentDetails']['videoId'],response['items']))

        videoPublishedAt = list(map(lambda x: x['contentDetails']['videoPublishedAt'],response['items']))

        content_details = {'videoId': videoId,'Date':videoPublishedAt}


        #video Stats
        url2 = "https://www.googleapis.com/youtube/v3/videos"
        video_response =[]

        for i in videoId:
            param2 = {
                'access_token':access_token,
                'part':'id,statistics',
                'id' : i
                }
            
            
            video_response.append(requests.get(url2,params=param2).json())

        video_stats = list(map(lambda x: x['items'][0]['statistics'],video_response))
        video_id = list(map(lambda x: x['items'][0]['id'],video_response))

        Stats = {'id':video_id, 
                 'Views' : list(map(lambda x: x['viewCount'],video_stats)),
                 'Likes' : list(map(lambda x: x['likeCount'],video_stats)),
                 'Dislikes' : list(map(lambda x: x['dislikeCount'],video_stats)),
                 'Favorites' : list(map(lambda x: x['favoriteCount'],video_stats)),
                 'Number_of_Comments' : list(map(lambda x: x['commentCount'],video_stats))

                 }

        return Stats,content_details
    
    
    
    
from google.cloud import bigquery
import os 


class Bigquery():
    def __init__(self,BigQuery_credential):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= BigQuery_credential

    def check_bq_table_exists(self, dataset_id, table_name):

        table_ref = self.client.dataset(dataset_id).table(table_name)
        try:
            self.client.get_table(table_ref)
            return True
        except:
            return "Table already exists"

    
    def dump_into_bigquery(self,dataset,youtube_data,info_table,stats_table):
        
        stats,content_details = youtube_data
        
        client = bigquery.Client()
        try:
            dataset = client.create_dataset(dataset)
        except:
            
            print('dataset already exists')
        
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
                
                bigquery.SchemaField("Views", bigquery.enums.SqlTypeNames.INTEGER),
        
                bigquery.SchemaField("Likes", bigquery.enums.SqlTypeNames.INTEGER),
        
                bigquery.SchemaField("Dislikes", bigquery.enums.SqlTypeNames.INTEGER),
        
                bigquery.SchemaField("Favorites", bigquery.enums.SqlTypeNames.INTEGER),
        
                bigquery.SchemaField("Number_of_Comments", bigquery.enums.SqlTypeNames.INTEGER),
            ],
        )


        client.load_table_from_json(stats,f"{dataset}.{stats_table}",job_config=job_config)



        job_config2 = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("videoId", bigquery.enums.SqlTypeNames.STRING),
                                                     
                bigquery.SchemaField("Date", bigquery.enums.SqlTypeNames.DATETIME)
            ])


        client.load_table_from_json(content_details,f"{dataset}.{info_table}",job_config=job_config2)
        
        print("Data has been dumped into bigquery")





    
    