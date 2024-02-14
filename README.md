YouTube Data Harvesting and Warehousing

1.0 Overview:

This project aims to harvest data from YouTube and warehouse it for further analysis and insights. By collecting various types of data from YouTube, such as videos, channels, comments, and metadata, this project provides a comprehensive dataset for researchers, analysts, and developers to explore and derive valuable insights.

2.0 Features:

2.1 Data Harvesting: Utilizes YouTube API to gather information on videos, channels, comments, etc.
  
2.2 Data Warehousing: Stores harvested data in a structured format for easy        access and analysis.

2.3 Customizable Queries: Enables users to specify criteria for data collection based on their research needs.

2.4 Scheduled Harvesting: Supports automated scheduling of data harvesting tasks to ensure regular updates.

2.5 Data Integrity: Implements measures to maintain data integrity and consistency throughout the warehousing process.

2.6 Scalability: Designed to handle large volumes of data efficiently, ensuring scalability as the dataset grows.

2.7 Documentation: Provides comprehensive documentation on usage, configuration, and data schema for ease of understanding and integration.


3.0 Getting Started:

3.1 Prerequisites: Ensure you have access to the YouTube API and necessary API keys.

3.2 Installation:
  
    pip install google-api-python-client
    pip install pymongo
    pip install pandas
    pip install psycopg2
    pip install streamlit
    
3.3 Features: 
a.	Retrieve data from the YouTube API, including channel information, playlists, videos, and comments.

b.	Store the retrieved data in a MongoDB database.

c.	Migrate the data to a postgreSQL data warehouse.

d.	Analyze and visualize data using Streamlit.

e.	Perform queries on the postgreSQL data warehouse.

f.	Display the list of channel name's along with subcription & view's count.


3.4 Retrieving data from the YouTube API:

The project utilizes the Google API to retrieve comprehensive data from YouTube channels. The data includes information on channels, playlists, videos, and comments.

3.5 Storing data in MongoDB:

The retrieved data is stored in a MongoDB database . Before storing the data in mongodb we used the one of the Option to check wheather the respective channel Data is exist or Not in MongoDb.

3.6 Migrating data to a MySQL data warehouse:

The application allows users to migrate data from MongoDB to a postgreSQL data warehouse. Users can given the input as channel id to migrate the data to postgresql database. Following data cleaning, the information is segregated into separate tables, including channels, videos, and comments it can be retreived by SQL queries.

3.7 Visualize the stored data using Streamlit:

This code allows you to view the extracted data in streamlit application by running the below code in vitrual enviroinment.

			streamlit run youtube.py

