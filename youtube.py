from googleapiclient.discovery import build
import pandas as pd
import pymongo
import mysql.connector
import streamlit as st
import re
from datetime import timedelta


# API Connection

def Api_connect():
    Api_Id="AIzaSyAV979g8vArHRVWgpxfzVafCmPOZRKiHDY"
    
    api_service_name="youtube"
    api_version="v3"
    
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    
    return youtube

youtube=Api_connect()


# get Channel information

def get_channel_info(channel_id):
    request = youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
    )
    response = request.execute()

    for i in response['items']:
        data= dict(channel_name=i['snippet']['title'],
                Channel_Id=i['id'],
                Subscribers=i['statistics']['subscriberCount'],
                view_count=i['statistics']['viewCount'],
                Total_videos=i['statistics']['videoCount'],
                channel_Description=i['snippet']['description'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads']
                )
        return data


# get playlist details

def get_playlist_info(channel_id):
    All_datas=[]
    next_page_token= None
    next_page=True
    while next_page:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'Playlist_Id':item['id'],
                    'Title':item['snippet']['title'],
                    'Channel_Id':item['snippet']['channelId'],
                    'Channel_Name':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'Video_Count':item['contentDetails']['itemCount']}        
            All_datas.append(data)   
        
        next_page_token=response.get('next_page_token')
        if next_page_token is None:
            next_page=False
    return All_datas


# get video ids

def get_videos_ids(channel_id):

    video_ids=[]

    response=youtube.channels().list(id=channel_id,
                                    part="contentDetails").execute()

    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None

    while True:

        response1=youtube.playlistItems().list(
                                            part="snippet",
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            
        next_page_token=response1.get('nextPageToken')
        
        if next_page_token is None:
            break
        
    return video_ids


# get video information

def get_video_info(video_ids):
    video_data=[]
    
    for video_id in video_ids:
        request = youtube.videos().list(
                                part="snippet,contentDetails,statistics",
                                id=video_id
        )
        response = request.execute()
        
        for i in response['items']:
            data=dict(Channel_Name=i['snippet']['channelTitle'],
                     Channel_Id=i['snippet']['channelId'],
                     Video_Id=i['id'],
                     Title=i['snippet']['title'],
                     Thumbnail=i['snippet']['thumbnails']['default']['url'],
                     Description=i['snippet'].get('description'),
                     Published_Date=i['snippet']['publishedAt'],
                     Duration=i['contentDetails']['duration'],
                     Views=i['statistics'].get('viewCount'),
                     Likes=i['statistics'].get('likeCount'),
                     Comments=i['statistics'].get('commentCount'),
                     Favorite_Count=i['statistics']['favoriteCount'],
                     Definition=i['contentDetails']['definition'],
                     Caption_Status=i['contentDetails']['caption'])
            video_data.append(data)
    return video_data

#get comment details

def get_comment_info(video_ids):
    
    comment_data=[]
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                                                    part="snippet",
                                                    videoId=video_id,
                                                    maxResults=50
            )
            response = request.execute()

            for i in response['items']:
                data= dict(Comment_id=i['snippet']['topLevelComment']['id'],
                            Video_Id=i['snippet']['videoId'],
                            Comment_Text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=i['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_data.append(data)
        
    except:
        pass
    
    return comment_data


# MongoDb connection

con = pymongo.MongoClient("mongodb://localhost:27017/")
db = con["youtube_data"]


# upload to MongoDb

def Channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_info(channel_id)
    vd_ids=get_videos_ids(channel_id)
    vd_details=get_video_info(vd_ids)
    com_details=get_comment_info(vd_ids)
    
    coll = db["channel_details"]
        
    coll.insert_one({"Channel_information":ch_details,"playlist_information":pl_details,
                     "video_information":vd_details,"comment_information":com_details})
        
    return "upload completed successfully"


#Table creation for channels,playlist,videos,comments

def channels_table():
    
#mysql connection 

    mydb = mysql.connector.connect(host='localhost',user='root',password='Kavi@245',database='youtube_data')
    mycursor = mydb.cursor()

# drop query

    drop_sql_query='''drop table if exists channels'''
    mycursor.execute(drop_sql_query)
    mydb.commit()

# create channels table

    try:
        sql='''create table if not exists channels(channel_name varchar(80),
                                                    Channel_Id varchar(80) primary key,
                                                    Subscribers bigint,
                                                    view_count bigint,
                                                    Total_videos int,
                                                    channel_Description text,
                                                    Playlist_Id varchar(80))'''
        mycursor.execute(sql)
        mydb.commit()

    except:
        print("Channel Table already Created")

    # Get data from MongoDb

    channel_list=[]
    db = con["youtube_data"]
    coll = db["channel_details"] 
    for channel_data in coll.find({},{"_id":0,"Channel_information":1}):
        channel_list.append(channel_data["Channel_information"])
    df = pd.DataFrame(channel_list)

    # insert channels row in table

    for index,row in df.iterrows():
        insert_sql_query='''insert into channels(channel_name,
                                                Channel_Id,
                                                Subscribers,
                                                view_count,
                                                Total_videos,
                                                channel_Description,
                                                Playlist_Id)
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
                                                
            
        values=(row['channel_name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['view_count'],
                row['Total_videos'],
                row['channel_Description'],
                row['Playlist_Id'])

        try:
            mycursor.execute(insert_sql_query,values)
            mydb.commit()
            
        except:
            print("Channels values are already inserted")   




def playlists_table():

    mydb = mysql.connector.connect(host='localhost',user='root',password='Kavi@245',database='youtube_data')
    mycursor = mydb.cursor()

    drop_query = "drop table if exists playlists"
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists playlists(Playlist_Id varchar(255) primary key,
                                                                Title varchar(80), 
                                                                Channel_Id varchar(255), 
                                                                Channel_name varchar(255),
                                                                PublishedAt varchar(255),
                                                                Video_Count int)'''
        
        mycursor.execute(create_query)
        mydb.commit()
        
    except:
        print("Playlists Table alraedy created")    


    db = con["youtube_data"]
    coll =db["channel_details"]
    playlist_list = []
    for playlist_data in coll.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(playlist_data["playlist_information"])):
            playlist_list.append(playlist_data["playlist_information"][i])
    df1 = pd.DataFrame(playlist_list)



    for index,row in df1.iterrows():
        insert_sql_query = '''insert into playlists(Playlist_Id,
                                                    Title,
                                                    Channel_Id,
                                                    Channel_Name,
                                                    PublishedAt,
                                                    Video_Count)
                                        
                                                    values(%s,%s,%s,%s,%s,%s)'''            
        
        values =(row['Playlist_Id'],
                 row['Title'],
                 row['Channel_Id'],
                 row['Channel_Name'],
                 row['PublishedAt'],
                 row['Video_Count'])
                
        try:                     
            mycursor.execute(insert_sql_query,values)
            mydb.commit()    
        except:
            print("Playlists values are already inserted")



# Function to convert duration string

def convert_duration(duration_str):
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(\d+)S', duration_str)
    if match:
        hours, minutes, seconds = map(int, match.groups(default='0'))
        duration = timedelta(hours=hours, minutes=minutes, seconds=seconds)
        formatted_duration = str(duration)
        return formatted_duration
    else:
        return None


def videos_table():
    mydb = mysql.connector.connect(host='localhost',user='root',password='Kavi@245',database='youtube_data')
    mycursor = mydb.cursor()

    drop_query = "drop table if exists videos"
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists videos(Channel_Name varchar(255),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(100) primary key,
                                                        Title varchar(150),
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Published_Date varchar(255),
                                                        Duration varchar(10),
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_Count bigint,
                                                        Definition varchar(100), 
                                                        Caption_Status varchar(50)
                                                        )''' 
                                        
        mycursor.execute(create_query)             
        mydb.commit()

    except:
        print("Videos Table already created")
        


    video_list = []
    db = con["youtube_data"]
    coll = db["channel_details"]
    for video_data in coll.find({},{"_id":0,"video_information":1}):
        for i in range(len(video_data["video_information"])):
            video_list.append(video_data["video_information"][i])
    df2 = pd.DataFrame(video_list)



    mydb = mysql.connector.connect(host='localhost',user='root',password='Kavi@245',database='youtube_data')
    mycursor = mydb.cursor()



    for index,row in df2.iterrows():
            # Convert 'Duration' to HH:MM:SS format
            formatted_duration = convert_duration(row['Duration'])
            
            insert_query ='''insert into videos(Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Thumbnail,
                                                Description,
                                                Published_Date,
                                                Duration,
                                                Views,
                                                Likes,
                                                Comments,
                                                Favorite_Count,
                                                Definition,
                                                Caption_Status)
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    formatted_duration,
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status']
                    )
                    
            mycursor.execute(insert_query,values)
            mydb.commit()



def comments_table():

    mydb = mysql.connector.connect(host='localhost',user='root',password='Kavi@245',database='youtube_data')
    mycursor = mydb.cursor()



    drop_query = "drop table if exists comments"
    mycursor.execute(drop_query)
    mydb.commit()



    try:
        create_sql_query = '''create table if not exists comments(Comment_id varchar(100) primary key,
                                                                  Video_Id varchar(80),
                                                                  Comment_Text text,
                                                                  Comment_Author varchar(150),
                                                                  Comment_Published varchar(100))'''
        mycursor.execute(create_sql_query)
        mydb.commit()
        
    except:
        print("Comments Table already created")

    comment_list = []
    db = con["youtube_data"]
    coll = db["channel_details"]
    for comment_data in coll.find({},{"_id":0,"comment_information":1}):
        for i in range(len(comment_data["comment_information"])):
            comment_list.append(comment_data["comment_information"][i])
    df3 = pd.DataFrame(comment_list)


    for index, row in df3.iterrows():
        insert_query = '''insert into comments(Comment_id,
                                               Video_Id,
                                               Comment_Text,
                                               Comment_Author,
                                               Comment_Published)
                                               values (%s,%s,%s,%s,%s)'''

        values=(row['Comment_id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published'])
        
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("This comments are already exist in comments table")



def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Tables Created successfully"


    
def show_channels_table():
    channel_list = []
    db = con["youtube_data"]
    coll = db["channel_details"] 
    for channel_data in coll.find({},{"_id":0,"Channel_information":1}):
        channel_list.append(channel_data["Channel_information"])
    df = st.dataframe(channel_list)
    return df

def show_playlists_table():
    db = con["youtube_data"]
    coll =db["channel_details"]
    playlist_list = []
    for playlist_data in coll.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(playlist_data["playlist_information"])):
                playlist_list.append(playlist_data["playlist_information"][i])
    df1 = st.dataframe(playlist_list)
    return df1

def show_videos_table():
    video_list = []
    db = con["youtube_data"]
    coll = db["channel_details"]
    for video_data in coll.find({},{"_id":0,"video_information":1}):
        for i in range(len(video_data["video_information"])):
            video_list.append(video_data["video_information"][i])
    df2 = st.dataframe(video_list)
    return df2

def show_comments_table():
    comment_list = []
    db = con["youtube_data"]
    coll = db["channel_details"]
    for comment_data in coll.find({},{"_id":0,"comment_information":1}):
        for i in range(len(comment_data["comment_information"])):
            comment_list.append(comment_data["comment_information"][i])
    df3 = st.dataframe(comment_list)
    return df3


with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")
    
channel_id = st.text_input("Enter the Channel Id")


if st.button("Collect and Store data"):
    
    channel_ids = []
    db = con["youtube_data"]
    coll = db["channel_details"]
    for channel_data in coll.find({},{"_id":0,"Channel_information":1}):
        channel_ids.append(channel_data["Channel_information"]["Channel_Id"])
    if channel_id in channel_ids:
        st.success("Channel details of the given channel id is already exists")
    else:
        insert = Channel_details(channel_id)
        st.success(insert)
            
if st.button("Migrate to SQL"):
    display = tables()
    st.success(display)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table == "CHANNELS":
    show_channels_table()
elif show_table == "PLAYLISTS":
    show_playlists_table()
elif show_table =="VIDEOS":
    show_videos_table()
elif show_table == "COMMENTS":
    show_comments_table()
    
    
mydb = mysql.connector.connect(host='localhost',user='root',password='Kavi@245',database='youtube_data')
mycursor = mydb.cursor()

question = st.selectbox('Please Select Your Question',('1. All the videos and the Channel Name',
                                                        '2. Channels with most number of videos',
                                                        '3. 10 most viewed videos',
                                                        '4. Comments in each video',
                                                        '5. Videos with highest likes',
                                                        '6. likes of all videos',
                                                        '7. views of each channel',
                                                        '8. videos published in the year 2022',
                                                        '9. average duration of all videos in each channel',
                                                        '10. videos with highest number of comments'))

mydb = mysql.connector.connect(host='localhost',user='root',password='Kavi@245',database='youtube_data')
mycursor = mydb.cursor()

if question == '1. All the videos and the Channel Name':
    query1='''select Title as videos,Channel_Name as ChannelName from videos;'''
    mycursor.execute(query1)
    q1=mycursor.fetchall()
    st.write(pd.DataFrame(q1,columns=["Video Title","Channel Name"]))
    
elif question == '2. Channels with most number of videos':
    query2 = '''select channel_name as ChannelName,Total_videos as NO_Videos from channels order by Total_Videos desc;'''
    mycursor.execute(query2)
    q2=mycursor.fetchall()
    st.write(pd.DataFrame(q2,columns=["Channel Name","No Of Videos"]))
    
elif question == '3. 10 most viewed videos':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    mycursor.execute(query3)
    q3 = mycursor.fetchall()
    st.write(pd.DataFrame(q3, columns = ["views","channel Name","video title"]))
    
elif question == '4. Comments in each video':
    query4 = '''select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;'''
    mycursor.execute(query4)
    q4=mycursor.fetchall()
    st.write(pd.DataFrame(q4, columns=["No Of Comments", "Video Title"]))
    
elif question == '5. Videos with highest likes':
    query5 = '''select Title as VideoTitle,Channel_Name as ChannelName, Likes as LikesCount from videos 
                        where Likes is not null order by Likes desc;'''
    mycursor.execute(query5)
    q5 = mycursor.fetchall()
    st.write(pd.DataFrame(q5, columns=["video Title","channel Name","like count"]))
    
elif question == '6. likes of all videos':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    mycursor.execute(query6)
    q6 = mycursor.fetchall()
    st.write(pd.DataFrame(q6, columns=["like count","video title"]))
    
elif question == '7. views of each channel':
    query7 = "select channel_name as ChannelName, view_count as Channelviews from channels;"
    mycursor.execute(query7)
    q7=mycursor.fetchall()
    st.write(pd.DataFrame(q7, columns=["channel name","total views"]))
    
elif question == '8. videos published in the year 2022':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                    where extract(year from Published_Date) = 2022;'''
    mycursor.execute(query8)
    q8=mycursor.fetchall()
    st.write(pd.DataFrame(q8,columns=["Name", "Video Publised On", "ChannelName"]))
    
elif question == '9. average duration of all videos in each channel':
    query9 = '''SELECT Channel_Name as ChannelName, 
            AVG(TIME_TO_SEC(STR_TO_DATE(Duration, '%H:%i:%s'))) AS average_duration_seconds 
            FROM videos WHERE Duration IS NOT NULL GROUP BY Channel_Name;'''

    mycursor.execute(query9)
    q9 = mycursor.fetchall()

    # Convert the query results into a DataFrame
    df9 = pd.DataFrame(q9, columns=['ChannelName', 'average_duration_seconds'])

    # Initialize an empty list to store the formatted results
    Q9 = []
    # Iterate over the DataFrame rows
    for index, row in df9.iterrows():
        channel_name = row["ChannelName"]
        average_duration_seconds = row["average_duration_seconds"]

        # Check if average_duration_seconds is not None
        if average_duration_seconds is not None:
            # Convert seconds to HH:MM:SS format
            hours, remainder = divmod(average_duration_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            average_duration_str = "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(seconds))
        else:
            average_duration_str = None

        # Append the formatted results to the list
        Q9.append(dict(ChannelName=channel_name, AverageDuration=average_duration_str))
    
    # Create a new DataFrame from the formatted results
    result_df9 = pd.DataFrame(Q9)
    
    # Display the result DataFrame
    st.write(result_df9)

    
elif question == '10. videos with highest number of comments':
    query10 = '''SELECT Channel_Name, Video_Id, Title, Comments FROM videos
            WHERE Comments IS NOT NULL ORDER BY Comments DESC;'''
    mycursor.execute(query10)
    q10 = mycursor.fetchall()
    st.write(pd.DataFrame(q10, columns=['Channel_Name', 'Video_Id', 'Title', 'Comments']))