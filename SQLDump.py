# Dump in sql Server

from ExtractYTdata import YT_data_extraction
import sqlalchemy

stats_df,content_details = YT_data_extraction()

SERVER = 'DESKTOP-7KG2T6S'
DATABASE = 'testing'
DRIVER = 'SQL Server Native Client 11.0'

DATABASE_CONNECTION = f'mssql://@{SERVER}/{DATABASE}?driver={DRIVER}'

engine = sqlalchemy.create_engine(DATABASE_CONNECTION,echo = True)

conn = engine.connect()


con_dtype = {'videoId':sqlalchemy.String(),'Date':sqlalchemy.Date()}
content_details.to_sql("content_details",engine,if_exists='replace',index=False)



dtype = {'id':sqlalchemy.String(),'Views':sqlalchemy.Integer(),
         'Likes':sqlalchemy.Integer(),'Dislikes':sqlalchemy.Integer(),'Favorites':sqlalchemy.Integer(),
         'Number_of_Comments':sqlalchemy.Integer()}


stats_df.to_sql("video_stats",engine,if_exists='replace',index=False,dtype=dtype)





