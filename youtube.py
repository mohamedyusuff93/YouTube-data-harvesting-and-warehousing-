from googleapiclient.discovery import build
from streamlit_option_menu import option_menu
import pymongo
import pandas as pd
import psycopg2
import streamlit as st
from PIL import Image

def Api_connect():
    api_key='AIzaSyDuVxPrTcUMPvyvchTM63JRLTTBKkkyoqE'
    api_version="v3"
    api_service_name="youtube"
    youtube=build(api_service_name,api_version,developerKey=api_key)
    return youtube
youtube=Api_connect()
def channel_details(channel_id):
    response=youtube.channels().list(id=channel_id,
                                   part="snippet, contentDetails,statistics")
    request=response.execute()
    for i in request['items']:
        data=dict(Channel_name=i['snippet']['title'],
                  Channel_id=i['id'],
                  Subscribers_Count=i['statistics']['subscriberCount'],
                  Videos_Count=i['statistics']['videoCount'],
                  Views_Count=i['statistics']['viewCount'],
                  Playlist_id=i['contentDetails']['relatedPlaylists']['uploads'],
                  Channel_Description=i['snippet']['description'])
    return data

def get_video_ids(channel_id):
    v_id=[]
    response=youtube.channels().list(id=channel_id,part='ContentDetails').execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    
    next_page_token=None

    while True:
        request1=youtube.playlistItems().list(part="snippet,contentDetails",pageToken=next_page_token,maxResults=50,playlistId=playlist_id).execute()
        for i in range(len(request1['items'])):
            v_id.append(request1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=request1.get('nextPageToken')
        if next_page_token is None:
            break
    return v_id

def get_video_details(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(id=video_id,part='snippet,statistics,contentDetails')
        response=request.execute()
        for item in response['items']:
            data=dict(Channel_name=item['snippet']['channelTitle'],
                      Channel_id=item['snippet']['channelId'],
                      Video_id=item['id'],
                      Title=item['snippet']['title'],
                      Video_description=item['snippet'].get('description'),
                      Video_tag=item['snippet'].get('tags'),
                      View_count=item['statistics'].get('viewCount'),
                      Like_count=item['statistics'].get('likeCount'),
                      Comment_count=item['statistics'].get('commentCount'),
                      Favorite_count=item['statistics']['favoriteCount'],
                      Duration_Id=item['contentDetails']['duration'],
                      Defination=item['contentDetails']['definition'],
                      Caption_status=item['contentDetails']['caption'],
                      Published_date_time=item['snippet']['publishedAt'],
                      Thumbnail=item['snippet']['thumbnails']['default']['url'])
            video_data.append(data)
    return video_data

def get_comment_details(video_ids):
  comment_details=[]
  try:
    for video_id in video_ids:
      request=youtube.commentThreads().list(part='snippet',videoId=video_id,maxResults=50)
      response=request.execute()
      for item in response['items']:
        data=dict(Comment_id=item['snippet']['topLevelComment']['id'],
                  Video_id=item['snippet']['videoId'],
                  Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                  Commented_by=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                  Commented_time=item['snippet']['topLevelComment']['snippet']['publishedAt']
                  )
        comment_details.append(data)
  except:
    pass
  return comment_details

client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["Youtube_details"]
def mongo_insert(channel_id):
    try:
        channel_info=channel_details(channel_id)
        video_Id=get_video_ids(channel_id)
        video_info=get_video_details(video_Id)
        comment_info=get_comment_details(video_Id)
        
        collection1=db['Channel_details']
        collection1.insert_one({"Channel_info":channel_info,"Video_information":video_info,"Comment_information":comment_info})
        return "Database stored in MongoDB"
    except:
        st.success("Enter the channel ID first")
def Channel_table_sql(select_channel):
    myconnection = psycopg2.connect(host='127.0.0.1',user='postgres',password='yusuff@12345',port='5432',database='youtube')
    cursor=myconnection.cursor()
    collection1=db['Channel_details']

    try:
        create_table='''create table if not exists channels(Channel_name varchar(100),
                                                             Channel_id varchar(100) primary key,
                                                             Subscribers_Count bigint,
                                                             Videos_Count bigint,
                                                             Views_Count bigint,
                                                             Playlist_id varchar(100),
                                                             Channel_Description text)'''
        cursor.execute(create_table)
        myconnection.commit()
    except:
        print("Channel table already created")
    
    mongo_find=collection1.find({"Channel_info.Channel_id":select_channel},{"_id":0,"Channel_info":1})

    try:
        for data in mongo_find:
            Channel_name=data["Channel_info"]['Channel_name']
            Channel_id=data["Channel_info"]['Channel_id']
            Subscribers_Count=data["Channel_info"]['Subscribers_Count']
            Videos_Count=data["Channel_info"]['Videos_Count']
            Views_Count=data["Channel_info"]['Views_Count']
            Playlist_id=data["Channel_info"]['Playlist_id']
            Channel_Description=data["Channel_info"]['Channel_Description']
            
            cursor.execute('''insert into channels(Channel_name,Channel_id,Subscribers_Count,Videos_Count,Views_Count,Playlist_id,Channel_Description)values(%s,%s,%s,%s,%s,%s,%s)''',(Channel_name,Channel_id,Subscribers_Count,Videos_Count,Views_Count,Playlist_id,Channel_Description))

            
                
            myconnection.commit()
    except:
        return "Table already created"
def videos_table():
    myconnection=psycopg2.connect(host='127.0.0.1',user='postgres',password='yusuff@12345',port='5432',database='youtube')
    cursor=myconnection.cursor()

    drop='''drop table if exists videos'''
    cursor.execute(drop)
    myconnection.commit()

    create_table='''create table if not exists videos(Channel_name varchar(100),
                                                      Channel_id varchar(100),
                                                      Video_id varchar(13) primary key,
                                                      Title varchar(150),
                                                      Video_description text,
                                                      Video_tag text,
                                                      View_count bigint,
                                                      Like_count bigint,
                                                      Comment_count int,
                                                      Favorite_count int,
                                                      Duration_Id interval,
                                                      Defination varchar(20),
                                                      Caption_status varchar(50),
                                                      Published_date_time timestamp,
                                                      Thumbnail varchar(200))'''
    cursor.execute(create_table)
    myconnection.commit()

    vi_list=[]
    db=client["Youtube_details"]
    collection1=db['Channel_details']
    for vi_data in collection1.find({},{"_id":0,"Video_information":1}):
        for i in range(len(vi_data['Video_information'])):
            vi_list.append(vi_data["Video_information"][i])
    df2=pd.DataFrame(vi_list)


    for index,row in df2.iterrows():
        insert_table='''insert into videos(Channel_name,
                                            Channel_id,
                                            Video_id,
                                            Title,
                                            Video_description,
                                            Video_tag,
                                            View_count,
                                            Like_count,
                                            Comment_count,
                                            Favorite_count,
                                            Duration_Id,
                                            Defination,
                                            Caption_status,
                                            Published_date_time,
                                            Thumbnail)
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['Channel_name'],
               row['Channel_id'],
               row['Video_id'],
               row['Title'],
               row['Video_description'],
               row['Video_tag'],
               row['View_count'],
               row['Like_count'],
               row['Comment_count'],
               row['Favorite_count'],
               row['Duration_Id'],
               row['Defination'],
               row['Caption_status'],
               row['Published_date_time'],
               row['Thumbnail'])
        cursor.execute(insert_table,values)
        myconnection.commit()
def comments_table():
    myconnection=psycopg2.connect(host='127.0.0.1',user='postgres',password='yusuff@12345',port='5432',database='youtube')
    cursor=myconnection.cursor()

    drop='''drop table if exists comments'''
    cursor.execute(drop)
    myconnection.commit()

    create_table='''create table if not exists comments(Comment_id varchar(100) primary key,
                                                      Video_id varchar(100),
                                                      Comment_text text,
                                                      Commented_by varchar(150),
                                                      Commented_time timestamp)'''
    cursor.execute(create_table)
    myconnection.commit()

    comment_list=[]
    db=client["Youtube_details"]
    collection1=db['Channel_details']
    for vi_data in collection1.find({},{"_id":0,"Comment_information":1}):
        for i in range(len(vi_data['Comment_information'])):
            comment_list.append(vi_data["Comment_information"][i])
    df3=pd.DataFrame(comment_list)


    for index,row in df3.iterrows():
        insert_table='''insert into comments(Comment_id,
                                            Video_id,
                                            Comment_text,
                                            Commented_by,
                                            Commented_time)
                                            values(%s,%s,%s,%s,%s)'''

        values=(row['Comment_id'],
               row['Video_id'],
               row['Comment_text'],
               row['Commented_by'],
               row['Commented_time'])
        cursor.execute(insert_table,values)
        myconnection.commit()
def create_insert_table(select_channel):
    Channel_table_sql(select_channel)
    videos_table()
    comments_table()
    
    return "Tables created and values inserted"
def channel_stream():
    channel_information=[]
    db=client["Youtube_details"]
    collection1=db['Channel_details']
    for ch_data in collection1.find({},{"_id":0,"Channel_info":1}):
        channel_information.append(ch_data["Channel_info"])
    df=st.dataframe(channel_information)
    return df
def videos_stream():
    vi_list=[]
    db=client["Youtube_details"]
    collection1=db['Channel_details']
    for vi_data in collection1.find({},{"_id":0,"Video_information":1}):
        for i in range(len(vi_data['Video_information'])):
            vi_list.append(vi_data["Video_information"][i])
    df2=st.dataframe(vi_list)
    return df2
def comments_stream():
    comment_list=[]
    db=client["Youtube_details"]
    collection1=db['Channel_details']
    for vi_data in collection1.find({},{"_id":0,"Comment_information":1}):
        for i in range(len(vi_data['Comment_information'])):
            comment_list.append(vi_data["Comment_information"][i])
    df3=st.dataframe(comment_list)
    return df3

#Streamlit section:


#st.title("Capstone Project")
st.markdown("<h1 style='text-align: center; color: white;font-family:system-ui'>Capstone Project</h1>", unsafe_allow_html=True)


    
#with st.sidebar:
    #st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    #st.header("Process")
selected=option_menu(
        menu_title="Youtube data harvesting and warehousing",
        options=["Ch ID","MongoDB","SQL","Tables","Questions"],
        orientation="horizontal",
        icons=[],
        menu_icon="youtube",
        default_index=0)
    
select_channel=st.text_input("Enter the channel ID")

if selected=="Ch ID":
    button1=st.button("Submit")
    #select_channel=st.text_input("Enter the channel ID")
    #st.title("Enter the channel ID")
    #title=st.text_input("Nothing entered")
    if button1==True:
        if len(select_channel)>=11:
            st.success("Channel id is valid, proceed to next step")
        else:
            st.success("Enter valid channel ID")

if selected=="MongoDB":
    button=st.button("Collect and store the data")
    if button==True:
        ch_id=[]
        db=client["Youtube_details"]
        coll1=db["Channel_details"]
        for ch_data in coll1.find({},{"_id":0,"Channel_info":1}):
            ch_id.append(ch_data["Channel_info"]["Channel_id"])
        if select_channel in ch_id:
            st.success("Channel already created and stored")
        else:
            mongo=mongo_insert(select_channel)
            st.success(mongo)
if selected=="SQL":
    button3=st.button("Migtrate to SQL")
    if button3==True:
        Table=create_insert_table(select_channel)
        st.success(Table)

if selected=="Tables":
    table_show=st.radio("Select the below anyone option to show the details of the selected option",("Channel Details","Video details","Comment details"))
    if table_show=="Channel Details":
        channel_stream()
    elif table_show=="Video details":
        videos_stream()
    elif table_show=="Comment details":
        comments_stream()
if selected=="Questions":
    myconnection=psycopg2.connect(host='127.0.0.1',user='postgres',password='yusuff@12345',port='5432',database='youtube')
    cursor=myconnection.cursor()
    questions=st.selectbox("Select your question",("1. What are the names of all the videos and their corresponding channels?",
                                                "2.Which channels have the most number of videos, and how many videos do they have?",
                                                "3.What are the top 10 most viewed videos and their respective channels?",
                                                "4.How many comments were made on each video, and what are their corresponding video names?",
                                                "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6.What is the total number of likes for each video, and what are their corresponding video names?",
                                                "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8.What are the names of all the channels that have published videos in the year 2022?",
                                                "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

    query1='''select title as videos,channel_name as channel from videos'''
    query2='''select count(video_id) as total_videos,channel_name as channel from videos group by channel_name'''
    query3='''select view_count as highest_view_count , channel_name as channel from videos where view_count is not null order by view_count desc limit 10'''
    query4='''select comment_count as no_of_comments , title as video_title from videos where comment_count is not null'''
    query5='''select like_count as likes , channel_name as channel from videos where like_count is not null order by like_count desc'''
    query6='''select sum(like_count) as total_likes, title as video_name from videos where like_count is not null group by title'''
    query7='''select sum(view_count) as views, channel_name as channel from videos where view_count is not null group by channel_name'''
    query8='''select channel_name as channel, published_date_time as Released_At from videos where extract(year from published_date_time) =2022'''
    query9='''select avg(duration_id) as average_duration,channel_name as channel from videos group by channel_name'''
    query10='''select comment_count as comments,title as video,channel_name as Channel from videos where comment_count is not null order by comment_count desc'''

    if questions=="1. What are the names of all the videos and their corresponding channels?":
        cursor.execute(query1)
        myconnection.commit()
        t1=cursor.fetchall()
        df=pd.DataFrame(t1,columns=['Video name','Channel name'])
        st.write(df)
    elif questions=="2.Which channels have the most number of videos, and how many videos do they have?":
        cursor.execute(query2)
        myconnection.commit()
        t2=cursor.fetchall()
        df1=pd.DataFrame(t2,columns=['Total videos','Channel name'])
        st.write(df1)
    elif questions=="3.What are the top 10 most viewed videos and their respective channels?":
        cursor.execute(query3)
        myconnection.commit()
        t3=cursor.fetchall()
        df2=pd.DataFrame(t3,columns=['Highest views','Channel name'])
        st.write(df2)
    elif questions=="4.How many comments were made on each video, and what are their corresponding video names?":
        cursor.execute(query4)
        myconnection.commit()
        t4=cursor.fetchall()
        df3=pd.DataFrame(t4,columns=['No of Comments','Video name'])
        st.write(df3)
    elif questions=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        cursor.execute(query5)
        myconnection.commit()
        t5=cursor.fetchall()
        df4=pd.DataFrame(t5,columns=['Likes count','Channel name',])
        st.write(df4)
    elif questions=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        cursor.execute(query6)
        myconnection.commit()
        t6=cursor.fetchall()
        df5=pd.DataFrame(t6,columns=['Total likes','Video name'])
        st.write(df5)
    elif questions=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
        cursor.execute(query7)
        myconnection.commit()
        t7=cursor.fetchall()
        df6=pd.DataFrame(t7,columns=['Views','Channel name'])
        st.write(df6)
    elif questions=="8.What are the names of all the channels that have published videos in the year 2022?":
        cursor.execute(query8)
        myconnection.commit()
        t8=cursor.fetchall()
        df7=pd.DataFrame(t8,columns=['Channel Name','Released date and time'])
        st.write(df7)
    elif questions=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        cursor.execute(query9)
        myconnection.commit()
        t9=cursor.fetchall()
        df8=pd.DataFrame(t9,columns=['Average Duration','Channel Name'])
        st.write(df8)
    elif questions=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
        cursor.execute(query10)
        myconnection.commit()
        t10=cursor.fetchall()
        df9=pd.DataFrame(t10,columns=['Comments count','Video name','Channel Name'])
        st.write(df9)
