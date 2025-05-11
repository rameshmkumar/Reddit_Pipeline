import praw
import os
from dotenv import load_dotenv
import pandas as pd
import time
from datetime import datetime
from sqlalchemy import create_engine
import logging

LOG_FILE='reddit_pipeline.log'
LOG_FORMAT='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d-%(message)s'

logging.basicConfig(level=logging.INFO, 
                    format=LOG_FORMAT, 
                    filename=LOG_FILE, 
                    filemode='a')

logging.info("Logging configured and Script is starting")
logging.info("Loading the .env variables")

load_dotenv()

#Reddit credentials
CLIENT_ID=os.getenv('REDDIT_CLIENT_ID')
CLIENT_SECRET=os.getenv('REDDIT_CLIENT_SECRET')
USER_AGENT=os.getenv('REDDIT_CLIENT_AGENT')
USERNAME=os.getenv('REDDIT_USERNAME')
PASSWORD=os.getenv('REDDIT_PASSWORD')

#Database Credentials
DB_USER=os.getenv('POSTGRES_USER')
DB_PASSWORD=os.getenv('POSTGRES_PASSWORD')
DB_HOST=os.getenv('POSTGRES_HOST')
DB_PORT=os.getenv('POSTGRES_PORT')
DB_NAME=os.getenv('POSTGRES_DB')


targeted_subreddits_str=os.getenv('TARGET_SUBREDDITS', )
post_limit_str=os.getenv('POST_LIMIT')
time_period_env=os.getenv('TIME_PERIOD')
table_name_env=os.getenv('POSTGRES_TABLE_NAME')


logging.info("Processing Configurations...")
try:   #first we split by ',' then look through it, then sub.strip() to remove the whitespaces
    targeted_subreddits=[sub.strip() for sub in targeted_subreddits_str.split(',') if sub.split()]
    if not targeted_subreddits:
        raise Valueerror 
except Exception as e:
    logging.warning(f"error: Getting the subreddits from .evn {e}")
    targeted_subreddits=['dataengineering','Python','Claude'] #Since loading from .env failed we use default subreddits

try:
    post_limit=int(post_limit_str)
    if post_limit <=0:
        raise Valueerror ("Post limit should be a positive integer")
except Exception as e:
    logging.warning("error converting post_limit_str to int. So using default limit: 10 ")
    post_limit=10

valid_time_periods=['hour','day','week','month','year','all']
if time_period_env.lower() not in valid_time_periods:
    logging.warning(f"Invalid time period {time_period_env} from .env file. using default: 'Day")
    time_period='day'
else:
    time_period=time_period_env.lower()

postgres_table_name=table_name_env
if not postgres_table_name:
    logging.warning("Postgres table name is being empty")

    postgres_table_name='reddit_posts' #using default table name for the error handling


db_engine=None


#Checking if all the credentials are interacting with .env file
if not all([DB_USER,DB_PASSWORD,DB_HOST,DB_PORT,DB_NAME]):   
    logging.warning("Missing postgres credentials from .env file")
else:
    try:
        DATABASE_URL=f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        db_engine=create_engine(DATABASE_URL)    #create_engine is a fun from sqlalchemy
        logging.info("Created DB engine successfully")
    except Exception as e:
        logging.error("error creating database engine")
        db_engine=None


#all() takes a iterable list and checks if all the elements are truthy
if not all([CLIENT_ID,CLIENT_SECRET,USER_AGENT,USERNAME,PASSWORD]):
    logging.error("error: Missing reddit credentials in .env file")
else:
    logging.info("credentials loaded successfully")


#let's try if we can authenticate our credentials with reddit
#PRAW - Python REDDIT API WRAPPER
#Creating instance with "praw.Reddit()". client_id,client_secret,user_agent,username,password are the parameters
try:
    reddit=praw.Reddit(client_id=CLIENT_ID, 
                       client_secret=CLIENT_SECRET,
                       user_agent=USER_AGENT,
                       username=USERNAME,
                       password=PASSWORD)
    logging.info(f"Authenticated as : {reddit.user.me()}")
    logging.info("Authentication is suceesful")

except Exception as e:
    logging.critical(f"error during Reddit authentication")
    reddit=None
    exit()

#read_only is a property of PRAW


#time_filter (str): Time filter for fetching top posts ('day', 'week', 'month', 'year', 'all')
#subreddits can be listed as a list
def extract_reddit_posts(reddit_instance, subreddits, limit=25,time_filter='day'):

    all_posts=[]

    #Printing empty list of all_posts, if reddit_instance was not created properly or reddit_instance is in read only mode
    if not reddit_instance or reddit_instance.read_only:
        logging.error("error: For Reddit instance or reddit_instance's permission")
    
    #looking through subreddits's list
    for subreddit_name in subreddits:
        try:
            subreddit=reddit_instance.subreddit(subreddit_name)  #We pull the subreddit

            posts=subreddit.top(time_filter=time_filter,limit=limit)  #From the subreddit, we pull the top posts

            count=0

            for post in posts:
                all_posts.append(post)
                count +=1
            logging.info(f"Fetched {count} of posts from r/{subreddit_name}")
            time.sleep(1)

        #This to catch the PRAW error
        except praw.exceptions.PRAWException as e:
            logging.error(f"error fetching from r/dataengineer {e}")

            continue         #continue to next subreddit

        #This is to catch the other erros
        except Exception as e:
            logging.error(f"Unexpected error was caught {e}")

            continue

    logging.info(f"Extraction has been completed. {len(all_posts)} posts were extracted")

    return all_posts

    


def transform_reddit_data(raw_all_posts):

    if not raw_all_posts:
        logging.warning(f"No post was detected to transform")
        return pd.DataFrame()
    
    transformed_posts=[]

    for post in raw_all_posts:
        try:
            author_name=None
            if post.author:
                author_name=post.author.name
            subreddit_display_name=None
            if post.subreddit:
                subreddit_display_name=post.subreddit.display_name
                              #'created_utc': datetime.utcfromtimestamp(post.created_utc),
            post_data={'post_id':post.id,
                   'title':post.title, 
                   'score':post.score, 
                   'num_comments':post.num_comments,
                   'created_utc': datetime.fromtimestamp(post.created_utc),
                   'author':author_name,
                   'subreddit':subreddit_display_name,
                   'url': post.url,
                   'selftext':post.selftext,
                   'is_self':post.is_self,
                   'permalink':f"www.reddit.com{post.permalink}"}

            transformed_posts.append(post_data)

        except Exception as e:
            logging.info(f"processing the post {e}")
            continue

    if not transformed_posts:
        logging.warning(f"No post was transformed")

    df=pd.DataFrame(transformed_posts)
    logging.info(f"Created dataframe with {len(df)} rows")
    
    if not df.empty:
        df['score']=pd.to_numeric(df['score'], errors='coerce').astype('Int64')
        df['author']=df['author'].astype('str')
        df['num_comments']=df['num_comments'].astype('Int64')
        df['is_self']=df['is_self'].astype('bool')
    return df

def load_data_db(df, table_name, db_engine):
    try:
        df.to_sql(table_name, db_engine, if_exists='replace', index=False, method='multi')
        
        return True
    except Exception as e:
        logging.error(f"error: loading into DB {e}")
        return False




if __name__=="__main__":

    logging.info("===== Starting Reddit ETL Pipeline====")

    if reddit:

        raw_data=extract_reddit_posts(reddit_instance=reddit, 
                                      subreddits=targeted_subreddits, 
                                      limit=post_limit, 
                                      time_filter=time_period)

        if raw_data:
            logging.info(f"Successfully extracted{len(raw_data)}")
            transformed_df=transform_reddit_data(raw_data)
            if transformed_df is not None:
                logging.info(f"Dataframe Head:\n{transformed_df.head(5)}")
                

                try:
                    transformed_df.to_csv('transformed_reddit_posts.csv', index=False)
                except Exception as e:
                    logging.error(f"error: while creating CSV file {e}")

                
                load_successful=load_data_db(transformed_df, postgres_table_name, db_engine)
                if load_successful:
                    logging.info("Successfully loaded into DB")
                else:
                    logging.warning("Loading to DB is failed")
            else:
                logging.error("Couldn't transformed data")

        else:
            logging.error("error fetching the posts")
    else:
        logging.error("error authenticating the reddit instance")




            
            


        




