# YouTube Data Harvesting and Warehousing

This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app.

NAME : Kaviya M
BATCH: DW73DW74
DOMAIN : DATA SCIENCE
LanGuage & Tool used: Python mongoDB pandas mysql

# SKILLS TAKEAWAY FROM THIS PROJECT
-> Python scripting -> Data Collection -> MongoDB -> SQL -> Streamlit -> API integration -> Data Management

# APPROACH
1. Set up a Streamlit app: Streamlit is a great choice for building data visualization and analysis tools quickly and easily. You can use Streamlit to create a simple UI where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse.
2. Connect to the YouTube API: You'll need to use the YouTube API to retrieve channel and video data. You can use the Google API client library for Python to make requests to the API.
3. Store data in a MongoDB data lake: Once you retrieve the data from the YouTube API, you can store it in a MongoDB data lake. MongoDB is a great choice for a data lake because it can handle unstructured and semi-structured data easily.
4. Migrate data to a SQL data warehouse: After you've collected data for multiple channels, you can migrate it to a SQL data warehouse. You can use a SQL database such as MySQL or PostgreSQL for this.
5. Query the SQL data warehouse: You can use SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input. You can use a Python SQL library such as SQLAlchemy to interact with the SQL database.
6. Display data in the Streamlit app: Finally, you can display the retrieved data in the Streamlit app. You can use Streamlit's data visualization features to create charts and graphs to help users analyze the data.

**Process Flow**

Obtain YouTube API credentials: Visit the Google Cloud Console.
Create a new project or select an existing project.
Enable the YouTube Data API v3 for your project.
Create API credentials for youtube API v3.

**ETL Process**

Extracting Data from youtube API.
Transforming data into the required format.
Loading Data into MYSQL




