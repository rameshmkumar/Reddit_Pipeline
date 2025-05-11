# 🛠 Reddit ETL Pipeline with Logging & Error Handling

This project is a robust Python pipeline that extracts, transforms, and loads (ETL) top posts from selected subreddits using the Reddit API (PRAW) and stores them in a PostgreSQL database. It was built as part of my self-improvement journey to sharpen my skills in automation, logging, error handling, and working with APIs and databases.

---

## 🚀 Features

- ✅ Extracts top Reddit posts from user-defined subreddits  
- ✅ Transforms post data into a clean, structured format  
- ✅ Loads the data into a PostgreSQL database  
- ✅ Extensive use of logging for tracking, debugging, and audit trails  
- ✅ Graceful error handling with fallbacks and helpful warnings  
- ✅ Uses environment variables for sensitive configurations  

---

## 📦 Tech Stack

- **Python**
- **PRAW** – Python Reddit API Wrapper
- **Pandas** – Data transformation
- **SQLAlchemy** – Database connectivity
- **PostgreSQL** – Data storage
- **dotenv** – Environment variable management
- **Logging** – For robust error tracking and transparency

---

## 📂 Environment Variables Required

Create a `.env` file in the root directory with the following:

```env
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_CLIENT_AGENT=your_user_agent
REDDIT_USERNAME=your_reddit_username
REDDIT_PASSWORD=your_reddit_password

POSTGRES_USER=your_db_user
POSTGRES_PASSWORD=your_db_password
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=your_database

TARGET_SUBREDDITS=dataengineering,Python,Claude
POST_LIMIT=10
TIME_PERIOD=day
POSTGRES_TABLE_NAME=reddit_posts
```

---

## 🧠 Why I Built This

- To **practice production-grade logging and exception handling**, especially in API and database workflows.  
- To **improve real-world coding discipline**, such as fallback defaults, validation, and traceable logs.  
- To **share what I’ve built** in case someone else is on the same learning path.

---

## 📝 Usage

1. Clone this repository  
2. Create a `.env` file (see above)  
3. Run the script:

```bash
python reddit_pipeline.py
```

4. View logs in `reddit_pipeline.log`

---

## 📌 Notes

- Includes fallback values if environment variables are missing  
- Avoids crashes using `try-except` blocks and logging  
- Can be extended into a full data pipeline with scheduling or dashboarding  

---

## 🔄 Planned Improvements

- Retry mechanism for API failures  
- Data validation and schema enforcement  
- Option to append to DB instead of replacing  
- Scheduled runs using cron or the `schedule` module  

---

Made with care & curiosity 🧠💡
