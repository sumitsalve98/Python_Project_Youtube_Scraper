import logging
import time
import urllib.parse
import mysql.connector as connection
import pandas as pd
import pymongo
from bs4 import BeautifulSoup
from flask import Flask, render_template, request, send_file
from flask_cors import cross_origin
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from sqlalchemy import create_engine
from pytube import YouTube
from pytube import Channel
from zipfile import ZipFile
import os.path
import requests



app = Flask(__name__)
@app.route('/',methods=['GET'])  # route to display the home page
@cross_origin()
def homePage():
    return render_template("index.html")



@app.route('/scrap',methods=['POST','GET']) # route to show yt data
@cross_origin()
def index():
    if request.method == 'POST':

        # creating log file
        try:
            log_file_name = "youtube_scrap_loggs.log"
            logging.basicConfig(
                level=logging.INFO,
                force=True,
                format='%(asctime)s %(levelname)-8s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename=log_file_name,
                filemode='w+'
            )
            logging.info("log file created")
            print("log file created")
        except Exception as e:
            logging.error(f"Error while creating log file. Error: {e}")
            print(f"Error while creating log file. Error: {e}")


        # recieving channel urls
        try:
            url_string = request.form['content'].replace(" ","")
            url_list = url_string.split(",")
            urls = []
            for i in url_list:
                i.strip()
                urls.append(i)
            logging.info(f"channel urls recieved successfully. urls = {urls}")
        except Exception as e:
            logging.error(f"Error while recieving urls. Error:{str(e)}")



        # taking user input about how many videos to scrap
        try:
            no_vid = request.form['no_of_vid'].replace(" ","")
            no_vid = no_vid.strip()
            no_vid = int(no_vid)
            print(type(no_vid))
            logging.info(f"{no_vid} videos are to be scraped")
        except Exception as e:
            logging.error(f"Error while recieving no_of_vid. Error:{str(e)}")


        try:
            no_com = request.form['no_of_comment'].replace(" ","")
            no_com = no_com.strip()
            no_com = int(no_com)
            print(type(no_com))
            logging.info(f"{no_com} comments are to be scraped")
        except Exception as e:
            logging.error(f"Error while recieving no_com. Error:{str(e)}")



        #creating empty csv files
        try:
            df = pd.DataFrame(columns=['youtuber_id', 'channel_name', 'subscribers', 'channel_url', 'instagram', 'facebook','linkedin', 'twitter'])
            df.to_csv('youtubers.csv', mode="w+", index=False)

            df = pd.DataFrame(columns=['youtuber_id', 'video_id','upload_date', 'title', 'views', 'likes', 'comments', 'description', 'length_in_min', 'url', 'Download'])
            df.to_csv('videos.csv', mode="w+", index=False)

            df = pd.DataFrame(columns=['video_id', 'comment_no', 'author_name', 'comment'])
            df.to_csv('comments.csv', mode="w+", index=False)
            logging.info("Empty csv files created")
            print("Empty csv files created")
        except Exception as e:
            logging.error(f"Error while creating empty csv files. Error: {e}")
            print(f"Error while creating empty csv files. Error: {e}")



        # connecting to sql database
        try:
            mydb = connection.connect(host="localhost",user="root",passwd="45515771",use_pure="True")
            cursor = mydb.cursor()  # create a cursor to execute queries
            query = "DROP DATABASE IF EXISTS youtube_scrap"
            cursor.execute(query)
            query = "CREATE DATABASE youtube_scrap"
            cursor.execute(query)
            mydb.commit()
            # mysql+mysqlconnector://user:passwd@host:port/dbname
            engine = create_engine('mysql+mysqlconnector://root:45515771@localhost:3306/youtube_scrap')
            logging.info("Successfully connected to sql database")
            print("Successfully connected to sql database")
        except Exception as e:
            logging.error(f"Error while connecting to sql. Error: {e}")
            print(f"Error while connecting to sql. Error: {e}")



        # connecting to mongodb database
        try:
            client = pymongo.MongoClient("mongodb+srv://sumitsalve:325978456566324@cluster0.4dylrww.mongodb.net/?retryWrites=true&w=majority")
            mongdb = client["scrap_youtube"]
            old_coll_list = mongdb.list_collection_names()
            new_collection_list = ['youtubers', 'videos', 'comments']
            for new_collection in new_collection_list:
                if new_collection in old_coll_list:
                    mongdb.drop_collection(new_collection)
                else:
                    pass

            mycoll1 = mongdb['youtubers']
            mycoll2 = mongdb['videos']
            mycoll3 = mongdb['comments']
            logging.info("Successfully connected to mongodb")
            print("Successfully connected to mongodb")
        except Exception as e:
            logging.error(f"Error while connecting to mongodb. Error: {e}")
            print(f"Error while connecting to mongodb. Error: {e}")


        # opening urls one by one
        for Id, Ytuber in enumerate(urls):
            Id_List = []
            Name_List = []
            Sub_count = []
            Channel_link = []
            Linkedin = []
            Insta_link = []
            Twitter_link = []
            Fb_link = []


            Id = Id+1
            Id_List.append(Id)

            option = webdriver.ChromeOptions()
            #option.add_argument("--headless")
            driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(), options=option)

            #opening about section of channel
            driver.get(f"{Ytuber}/about")
            driver.maximize_window()
            time.sleep(4)

            # scrolling down to end of page
            logging.info(f"About section opened for youtuber Id: {Id}")
            print(f"About section opened for youtuber Id: {Id}")
            driver.maximize_window()
            prev_h = 0
            while True:
                height = driver.execute_script("""
                            function getActualHeight() {
                                return Math.max(
                                    Math.max(document.body.scrollHeight, document.documentElement.scrollHeight),
                                    Math.max(document.body.offsetHeight, document.documentElement.offsetHeight),
                                    Math.max(document.body.clientHeight, document.documentElement.clientHeight)
                                );
                            }
                            return getActualHeight();
                        """)
                driver.execute_script(f"window.scrollTo({prev_h},{prev_h + 200})")
                # fix the time sleep value according to your network connection
                time.sleep(2)
                prev_h += 200
                if prev_h >= height:
                    break
            time.sleep(2)

            # storing page data in soup object
            soup = BeautifulSoup(driver.page_source, 'html.parser')


            # scraping social media links
            try:
                social_link = soup.findAll('a', class_='yt-simple-endpoint style-scope ytd-channel-about-metadata-renderer')
                for i in social_link:
                    slink = i['href']
                    slink = str(slink)
                    slink = slink.split("=")
                    # print(slink)
                    if len(slink) == 4:
                        slink = slink[3]
                        slink = urllib.parse.unquote(slink)

                        if "linkedin.com" in slink:
                            Linkedin.append(slink)

                        elif "instagram.com" in slink:
                            Insta_link.append(slink)

                        elif "twitter.com" in slink:
                            Twitter_link.append(slink)

                        elif "facebook.com" in slink:
                            Fb_link.append(slink)

                        elif "fb.com" in slink:
                            Fb_link.append(slink)

                        else:
                            pass

                    else:
                        pass

                if len(Linkedin) == 0:
                    Linkedin.append("NaN")
                else:
                    pass

                if len(Insta_link) == 0:
                    Insta_link.append("NaN")
                else:
                    pass

                if len(Twitter_link) == 0:
                    Twitter_link.append("NaN")
                else:
                    pass

                if len(Fb_link) == 0:
                    Fb_link.append("NaN")
                else:
                    pass

                logging.info(f"Successfully scraped social media links for youtuber id: {Id}")
                print(f"Successfully scraped social media links for youtuber id: {Id}")
            except Exception as e:
                logging.error(f"Error while scraping social media links for youtuber id: {Id}. Error: {str(e)}")
                print(f"Error while scraping social media links for youtuber id: {Id}. Error: {str(e)}")




            try:
                # Scrapping youtubers name
                youtubers = soup.findAll(id="text-container")[0].text.replace("\n", "").strip()
                Name_List.append(youtubers)
                logging.info(f"Youtubers name scraped for youtuber id: {Id}")
                print(f"Youtubers name scraped for youtuber id: {Id}")
            except Exception as e:
                logging.error(f"Error while scraping name for youtuber id: {Id}. Error: {str(e)}")
                print(f"Error while scraping name for youtuber id: {Id}. Error: {str(e)}")




            try:
                # Scrapping youtubers subscribers
                subs = soup.find(id='subscriber-count').text.replace("\n", "").strip()
                if subs.endswith('K subscribers'):
                    subs = subs.replace('K subscribers', '').strip()
                    subs = float(subs)
                    subs = subs * 1000
                    subs = int(subs)
                elif subs.endswith('M subscribers'):
                    subs = subs.replace('M subscribers', '').strip()
                    subs = float(subs)
                    subs = subs * 1000000
                    subs = int(subs)
                Sub_count.append(subs)
                logging.info(f"Youtubers subscriber count scraped for youtuber id: {Id}")
                print(f"Youtubers subscriber count scraped for youtuber id: {Id}")
            except Exception as e:
                logging.error(f"Error while scraping subscriber count for youtuber id: {Id}. Error: {str(e)}")
                print(f"Error while scraping subscriber count for youtuber id: {Id}. Error: {str(e)}")




            try:
                # scraping channel link
                channel_link = Ytuber.replace('videos', 'featured')
                Channel_link.append(channel_link)
                logging.info(f"Youtubers channel link scraped for youtuber id: {Id}")
                print(f"Youtubers channel link scraped for youtuber id: {Id}")
            except Exception as e:
                logging.error(f"Error while scraping channel link for youtuber id: {Id}")
                print(f"Error while scraping channel link for youtuber id: {Id}. Error: {str(e)}")




            try:
                # inserting data into csv , sql and mongo db
                data = {
                    "youtuber_id": Id_List,
                    "channel_name": Name_List,
                    "subscribers": Sub_count,
                    "channel_url": Channel_link,
                    "instagram": Insta_link,
                    "facebook": Fb_link,
                    "linkedin": Linkedin,
                    "twitter": Twitter_link
                }

                df = pd.DataFrame(data)

                df.to_csv('youtubers.csv', mode='a', index=False, header=False)
                logging.info(f"youtubers data inserted in youtubers.csv for youtuber Id: {Id}")
                print(f"youtubers data inserted in youtubers.csv for youtuber Id: {Id}")

                df.to_sql(name="youtubers", con=engine, if_exists="append", index=False)
                logging.info(f"youtubers data inserted in sql table youtubers for youtuber Id: {Id}")
                print(f"youtubers data inserted in sql table youtubers for youtuber Id: {Id}")

                mongodata = df.to_dict(orient="records")
                mycoll1.insert_many(mongodata)
                logging.info(f"youtubers data inserted in mongodb collection youtubers for youtuber Id: {Id}")
                print(f"youtubers data inserted in mongodb collection youtubers for youtuber Id: {Id}")
            except Exception as e:
                logging.error(f"Error while exporting data for youtubers table for youtuber Id: {Id}. Error: {str(e)}")
                print(f"Error while exporting data for youtubers table for youtuber Id: {Id}. Error: {str(e)}")


            # opening video section of schannel
            driver.get(f"{Ytuber}/videos")
            logging.info(f"Video section opened for youtuber Id: {Id}")
            print(f"Video section opened for youtuber Id: {Id}")
            time.sleep(5)

            if no_vid <= 30:
                pass
            else:
                maxscroll1 = no_vid * 40
                # scrolling down
                prev_h = 0
                while True:
                    height = driver.execute_script("""
                                        function getActualHeight() {
                                            return Math.max(
                                                Math.max(document.body.scrollHeight, document.documentElement.scrollHeight),
                                                Math.max(document.body.offsetHeight, document.documentElement.offsetHeight),
                                                Math.max(document.body.clientHeight, document.documentElement.clientHeight)
                                            );
                                        }
                                        return getActualHeight();
                                    """)
                    driver.execute_script(f"window.scrollTo({prev_h},{prev_h + 200})")
                    # fix the time sleep value according to your network connection
                    time.sleep(2)
                    prev_h += 200
                    if prev_h >= maxscroll1:
                        break

            time.sleep(2)

            soup = BeautifulSoup(driver.page_source, 'html.parser')


            # scrapping all title
            video_titles = soup.findAll('a', id='video-title')
            print(no_vid , len(video_titles))

            # itering through all titles
            for video_number, title in enumerate(video_titles[0:no_vid]):
                noshort = title["href"]
                noshort = str(noshort)
                if noshort[0:7] == '/shorts':
                    pass

                else:
                    # info of videos table
                    s_id_list = []
                    Video_Id = []
                    Title_Name = []
                    Views_list = []
                    Publish_date = []
                    Link_list = []
                    Download_link = []
                    Like_list = []
                    Total_comments = []
                    Length_in_min = []
                    Description = []



                    # info of comments table
                    T_Video_Id = []
                    Comments_no_list = []
                    Author_name = []
                    Comments_list = []



                    try:
                        s_id_list.append(Id)
                        logging.info(f"youtubers Id: {Id} is created for videos table")
                        print(f"youtubers Id: {Id} is created for videos table")
                    except Exception as e:
                        logging.error(f"Error while creating youtuber Id: {Id} in videos table. Error: {str(e)}")
                        print(f"Error while creating youtuber Id: {Id} in videos table. Error: {str(e)}")



                    try:
                        a = Id
                        a = str(a)
                        b = video_number+1
                        b = str(b)
                        video_id = a + 'of' + b
                        Video_Id.append(video_id)
                        logging.info(f"video Id: {video_id} is created in videos table")
                        print(f"video Id: {video_id} is created in videos table")
                    except Exception as e:
                        logging.error(f"Error while creating {video_id} in videos table. Error: {str(e)}")
                        print(f"Error while creating {video_id} in videos table. Error: {str(e)}")



                    try:
                        #scraping title name
                        Title_Name.append(title.text)
                        logging.info(f"video title is scraped successfully for video id: {video_id}")
                        print(f"video title is scraped successfully for video id: {video_id}")
                    except Exception as e:
                        logging.error(f"Error while scraping video title for video id: {video_id}. Error: {str(e)}")
                        print(f"Error while scraping video title for video id: {video_id}. Error: {str(e)}")



                    try:
                        #scraping video link
                        links = title["href"]
                        full_url = 'https://www.youtube.com' + links
                        Link_list.append(full_url)
                        logging.info(f"video link is scraped successfully for video id: {video_id}")
                        print(f"video link is scraped successfully for video id: {video_id}")
                    except Exception as e:
                        logging.error(f"Error while scraping video link for video id: {video_id}. Error: {str(e)}")
                        print(f"Error while scraping video link for video id: {video_id}. Error: {str(e)}")



                    try:
                        #creating download link
                        download_url = 'https://www.ssyoutube.com' + links
                        Download_link.append(download_url)
                        logging.info(f"download link is scraped successfully for video id: {video_id}")
                        print(f"download link is scraped successfully for video id: {video_id}")
                    except Exception as e:
                        logging.error(f"Error while creating download link for video id: {video_id}. Error: {str(e)}")
                        print(f"Error while creating download link for video id: {video_id}. Error: {str(e)}")



                    video_details = YouTube(full_url)
                    try:
                        # scraping publish date
                        Publish_date.append(video_details.publish_date)
                        logging.info(f"publish_date is scraped successfully for video id: {video_id}")
                        print(f"publish_date is scraped successfully for video id: {video_id}")
                    except Exception as e:
                        logging.error(f"Error while scraping publish_date for video id: {video_id}. Error: {str(e)}")
                        print(f"Error while scraping publish_date for video id: {video_id}. Error: {str(e)}")



                    try:
                        #scraping length of video
                        length_in_min = video_details.length
                        length_in_min = length_in_min/60
                        length_in_min = round(length_in_min, 2)
                        Length_in_min.append(length_in_min)
                        logging.info(f"length is scraped successfully for video id: {video_id}")
                        print(f"length is scraped successfully for video id: {video_id}")
                    except Exception as e:
                        logging.error(f"Error while scraping length for video id: {video_id}. Error: {str(e)}")
                        print(f"Error while scraping length for video id: {video_id}. Error: {str(e)}")



                    try:
                        # scraping description of video
                        Description.append(video_details.description)
                        logging.info(f"description is scraped successfully for video id: {video_id}")
                        print(f"description is scraped successfully for video id: {video_id}")
                    except Exception as e:
                        logging.error(f"Error while scraping description for video id: {video_id}. Error: {str(e)}")
                        print(f"Error while scraping description for video id: {video_id}. Error: {str(e)}")


                    # opening video from url
                    driver.get(full_url)
                    logging.info(f"video url is opened, video id: {video_id}")
                    print(f"video url is opened, video id: {video_id}")
                    time.sleep(5)
                    prev_h = 0
                    max = 800
                    while True:
                        height = driver.execute_script("""
                                                    function getActualHeight() {
                                                        return Math.max(
                                                            Math.max(document.body.scrollHeight, document.documentElement.scrollHeight),
                                                            Math.max(document.body.offsetHeight, document.documentElement.offsetHeight),
                                                            Math.max(document.body.clientHeight, document.documentElement.clientHeight)
                                                        );
                                                    }
                                                    return getActualHeight();
                                                """)
                        driver.execute_script(f"window.scrollTo({prev_h},{prev_h + 200})")
                        # fix the time sleep value according to your network connection
                        time.sleep(2)
                        prev_h += 200
                        if prev_h >= max:
                            break
                    time.sleep(2)
                    soup = BeautifulSoup(driver.page_source, 'html.parser')

                    try:
                        # scrapping views
                        view = soup.find(class_='view-count style-scope ytd-video-view-count-renderer').text.strip()
                        # print(view)
                        view = str(view)
                        view = view.replace('/n', '')
                        view = view.split()
                        view = view[0]
                        view = view.replace(',', '')
                        view = view.strip()
                        view = int(view)
                        # print(view)
                        Views_list.append(view)
                        logging.info(f"views are scraped successfully for video id: {video_id}")
                        print(f"views are scraped successfully for video id: {video_id}")
                    except Exception as e:
                        logging.error(f"Error while scraping views for video id: {video_id}. Error: {str(e)}")
                        print(f"Error while scraping views for video id: {video_id}. Error: {str(e)}")



                    try:
                        # scraping likes
                        likes = soup.find('a', class_= 'yt-simple-endpoint style-scope ytd-toggle-button-renderer').text
                        #print(likes)
                        likes = str(likes)
                        likes = likes.replace('/n', '')
                        likes = likes.split()
                        likes = likes[0]
                        likes = likes.strip()
                        #print(likes)
                        if likes.endswith('K'):
                            likes = likes.replace('K', '')
                            likes = float(likes)
                            likes = likes * 1000
                            Like_list.append(likes)
                        elif likes.endswith('M'):
                            likes = likes.replace('M', '')
                            likes = float(likes)
                            likes = likes * 1000000
                            likes = int(likes)
                            Like_list.append(likes)
                        else:
                            likes = int(likes)
                            Like_list.append(likes)
                        logging.info(f"likes are scraped successfully for video id: {video_id}")
                        print(f"likes are scraped successfully for video id: {video_id}")
                    except Exception as e:
                        logging.error(f"Error while scraping likes for video id: {video_id}. Error: {str(e)}")
                        print(f"Error while scraping likes for video id: {video_id}. Error: {str(e)}")



                    try:
                        # scraping total comments
                        total_comments = soup.find(class_='count-text style-scope ytd-comments-header-renderer').text
                        total_comments = str(total_comments)
                        total_comments = total_comments.replace('/n', '')
                        total_comments = total_comments.split()
                        total_comments = total_comments[0]
                        total_comments = total_comments.replace(',', '')
                        total_comments = total_comments.strip()
                        total_comments = int(total_comments)
                        Total_comments.append(total_comments)
                        logging.info(f"total comments are scraped successfully for video id: {video_id}")
                        print(f"total comments are scraped successfully for video id: {video_id}")
                    except Exception as e:
                        logging.error(f"Error while scraping total comments for video id: {video_id}. Error: {str(e)}")
                        print(f"Error while scraping total comments for video id: {video_id}. Error: {str(e)}")


                    try:
                        # inserting videos table data in csv, sql and mongodb
                        data = {
                            "youtuber_id": s_id_list,
                            "video_id": Video_Id,
                            "upload_date": Publish_date,
                            "title": Title_Name,
                            "views": Views_list,
                            "likes": Like_list,
                            "comments": Total_comments,
                            "description": Description,
                            "length_in_min": Length_in_min,
                            "url": Link_list,
                            "Download": Download_link
                        }

                        df = pd.DataFrame(data)
                        df.to_csv('videos.csv', mode='a', index=False, header=False)
                        logging.info(f"video data inserted in videos.csv for video id: {video_id}")
                        print(f"video data inserted in videos.csv for video id: {video_id}")

                        df.to_sql(name="videos", con=engine, if_exists="append", index=False)
                        logging.info(f"video data inserted in sql database for video id: {video_id}")
                        print(f"video data inserted in sql database for video id: {video_id}")

                        mongodata = df.to_dict(orient="records")
                        mycoll2.insert_many(mongodata)
                        logging.info(f"video data inserted in mongodb for video id: {video_id}")
                        print(f"video data inserted in mongodb for video id: {video_id}")
                    except Exception as e:
                        logging.error(f"Error while exporting video data in videos table for video id: {video_id}. Error: {str(e)}")
                        print(f"Error while exporting video data in videos table for video id: {video_id}. Error: {str(e)}")



                    # scrolling down depending on amounts of comments are available on video
                    if total_comments < no_com:
                        maxscroll = (total_comments * 100)+1000

                    if total_comments > no_com:
                        maxscroll = (no_com * 100)+1000


                    # opening video url second time to scrap comments
                    driver.get(full_url)
                    logging.info(f"video url is opened second time for video id: {video_id}")
                    print(f"video url is opened second time for video id: {video_id}")
                    time.sleep(4)
                    prev_h = 0
                    while True:
                        height = driver.execute_script("""
                                                    function getActualHeight() {
                                                        return Math.max(
                                                            Math.max(document.body.scrollHeight, document.documentElement.scrollHeight),
                                                            Math.max(document.body.offsetHeight, document.documentElement.offsetHeight),
                                                            Math.max(document.body.clientHeight, document.documentElement.clientHeight)
                                                        );
                                                    }
                                                    return getActualHeight();
                                                """)
                        driver.execute_script(f"window.scrollTo({prev_h},{prev_h + 200})")
                        # fix the time sleep value according to your network connection
                        time.sleep(2)
                        prev_h += 200
                        if prev_h >= maxscroll:
                            break
                    time.sleep(2)
                    soup = BeautifulSoup(driver.page_source, 'html.parser')


                    try:
                        # scraping name of commentor
                        authors = soup.findAll('a', id='author-text')
                        print(f"Total comment = {total_comments} ; Total scrapped comment,{len(authors)}")
                        for comment_number, author_name in enumerate(authors[0:no_com]):
                            comment_number = comment_number+1
                            Comments_no_list.append(comment_number)
                            T_Video_Id.append(video_id)
                            author_name = author_name.text
                            author_name = author_name.replace("\n", "")
                            author_name = author_name.strip()
                            Author_name.append(author_name)
                        logging.info(f"All authors names scraped for video id: {video_id}")
                        print(f"All authors names scraped for video id: {video_id}")
                    except Exception as e:
                        logging.error(f"Error while scraping author names for video id: {video_id}. Error: {str(e)}")
                        print(f"Error while scraping author names for video id: {video_id}. Error: {str(e)}")


                    try:
                        #scraping comment content
                        comments = soup.findAll(id='content-text')
                        for comment in comments[0:no_com]:
                            comment = comment.text
                            comment = comment.replace("\n", " ")
                            comment = comment.replace("  ", " ")
                            comment = comment.replace("   ", " ")
                            comment = comment.replace("    ", " ")
                            comment = comment.replace("     ", " ")
                            Comments_list.append(comment)
                        logging.info(f"All comments scraped for video id: {video_id}")
                        print(f"All comments scraped for video id: {video_id}")
                    except Exception as e:
                        logging.error(f"Error while scraping comments for video id: {video_id}. Error: {str(e)}")
                        print(f"Error while scraping comments for video id: {video_id}. Error: {str(e)}")


                    try:
                        # storing comments into csv, sql and mongodb.
                        data = {
                            "video_id": T_Video_Id,
                            "comment_no": Comments_no_list,
                            "author_name": Author_name,
                            "comment": Comments_list
                        }

                        df = pd.DataFrame(data)
                        df.to_csv('comments.csv', mode='a', index=False, header=False)
                        logging.info(f"video comments inserted in comments.csv for video id: {video_id}")
                        print(f"video comments inserted in comments.csv for video id: {video_id}")

                        df.to_sql(name="comments", con=engine, if_exists="append", index=False)
                        logging.info(f"comments inserted in sql database for video id: {video_id}")
                        print(f"comments inserted in sql database for video id: {video_id}")

                        mongodata = df.to_dict(orient="records")
                        mycoll3.insert_many(mongodata)
                        logging.info(f"comments inserted in mongodb for video id: {video_id}")
                        print(f"comments inserted in mongodb for video id: {video_id}")
                    except Exception as e:
                        logging.error(f"Error while exporting comments in comments table for video id: {video_id}. Error: {str(e)}")
                        print(f"Error while exporting comments in comments table for video id: {video_id}. Error: {str(e)}")

                    print("\n\n\n")

        driver.quit()
        mydb.close()

        # delet zip file if exist
        if os.path.exists('YoutubeData.zip'):
            os.remove('YoutubeData.zip')
        else:
            pass


        # Create a ZipFile Object
        with ZipFile('YoutubeData.zip', 'w') as zipObj:
            # Add multiple files to the zip
            zipObj.write('youtubers.csv')
            zipObj.write('videos.csv')
            zipObj.write('comments.csv')

        # render download link
        return render_template('results.html')

    else:
        return render_template('index.html')

# send download csv requiest in browser
@app.route('/download')
def download_file():
    path = 'YoutubeData.zip'
    return send_file(path, as_attachment= True)


if __name__ == "__main__":
    #app.run(host='127.0.0.1', port=8001, debug=True)
	app.run(debug=True)