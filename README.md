YouTube-Data-Harvesting-and-Warehousing-Project1

YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit.
Problem Statement: The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.

NAME : Kaviya M
BATCH: DW73DW74
DOMAIN : DATA SCIENCE
Lanuage & Tool used: Python mongoDB pandas mysql

YouTube_Scrapping
Introduction
This project is a YouTube API scrapper that allows users to retrieve and analyze data from YouTube channels. It utilizes the YouTube Data API to fetch information such as channel statistics, video details, comments, and more. The scrapper provides various functionalities to extract and process YouTube data for further analysis and insights.
Features
The YouTube Data Scraper offers a range of features to help you extract and analyze data from YouTube. Some of the key features include:
Retrieve channel statistics: Get detailed information about YouTube channels, including subscriber count, view count, video count, and other relevant metrics.
Fetch video details: Extract data such as video title, description, duration, view count, like count, dislike count, and publish date for individual videos.
Analyze comments: Retrieve comments made on YouTube videos and perform analysis, such as sentiment analysis or comment sentiment distribution.
Generate reports: Generate reports and visualizations based on the collected data, allowing users to gain insights into channel performance, video engagement, and audience interaction.
Data storage: Store the collected YouTube data in a database for easy retrieval and future reference.

Technologies Used

PYTHON: The project is implemented using the Python programming language.
YOUTUBE DATA API: 
Utilizes the official YouTube Data API to interact with YouTube's platform and retrieve data.
STREAMLIT: The user interface and visualization are created using the Streamlit framework, providing a seamless and interactive experience.
MongoDB: The collected data can be stored in a MongoDB database for efficient data management and querying.
MYSQL: A powerful relational database management system used to store and manage the retrieved data.
PYMANGO: 
A Python library that enables interaction with MongoDB, a NoSQL database. It is used for storing and retrieving data from MongoDB in the YouTube Data Scraper.
MYSQL:
 A MYSQL Connector for Python that allows seamless integration between Python and MySQL. It enables the YouTube Data Scraper to connect to and interact with the MYSQL database.
PANDAS: A powerful data manipulation and analysis library in Python. Pandas is used in the YouTube Data Scraper to handle and process data obtained from YouTube, providing functionalities such as data filtering, transformation, and aggregation.

Process Flow

Obtain YouTube API credentials: Visit the Google Cloud Console.
Create a new project or select an existing project.
Enable the YouTube Data API v3 for your project.
Create API credentials for youtube API v3.

ETL Process

Extracting Data from youtube API.
Transforming data into the required format.
Loading Data into MYSQL

Application Flow

Select Data Retrieval and Processing Page from dropdown menu at the sidebar.
Input the Channel Id and click on Get Channel Statistics in order to retrive data from Youtube API.
Next click on Push to MongoDB to store data in MongoDB Compass.
Select a channel name from the dropdown Channel Details and click on Push to SQL to import data into MYSQL.
Once imported, you can select the Analysis and Reports Page from the drop down to get a detailed analysis of the collected data.

Additional Information

Please note that when using this application, it is essential to comply with the YouTube Data API's terms of service and adhere to its usage limits to ensure uninterrupted access to the API. If you encounter any issues or have questions regarding the YouTube Data Scraper, please refer to the project's detailed documentation available in the GitHub repository.
