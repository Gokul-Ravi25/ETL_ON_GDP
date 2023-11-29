This ETL project is about

An international firm that is looking to expand its business in different countries across the world has recruited you. You have been hired as a junior Data Engineer and are tasked with creating an automated script that can extract the list of all countries in order of their GDPs in billion USDs (rounded to 2 decimal places), as logged by the International Monetary Fund (IMF). Since IMF releases this evaluation twice a year, this code will be used by the organization to extract the information as it is updated.

url: 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

Required Libraries

Before you start building the code, you need to install the required libraries for it.

The libraries needed for the code are as follows:

requests - The library used for accessing the information from the URL.

bs4 - The library containing the BeautifulSoup function used for webscraping.

pandas - The library used for processing the extracted data, storing it to required formats and communicating with the databases.

sqlite3 - The library required to create a database server connection.

numpy - The library required for the mathematical rounding operation as required in the objectives.

datetime - The library containing the function datetime used for extracting the timestamp for logging purposes.

Task 1: Extracting information
Extraction of information from a web page is done using the web scraping process. For this, you'll have to analyze the link and come up with the strategy of how to get the required information. The following points are worth observing for this task.

Inspect the URL and note the position of the table. Note that even the images with captions in them are stored in tabular format. Hence, in the given webpage, our table is at the third position, or index 2. Among this, we require the entries under 'Country/Territory' and 'IMF -> Estimate'.

Note that there are a few entries in which the IMF estimate is shown to be '—'. Also, there is an entry at the top named 'World', which we do not require. Segregate this entry from the others because this entry does not have a hyperlink and all others in the table do. So you can take advantage of that and access only the rows for which the entry under 'Country/Terriroty' has a hyperlink associated with it.

Note that '—' is a special character and not a general hyphen, '-'. Copy the character from the instructions here to use in the code.

Task 2: Transform information
The transform function needs to modify the ‘GDP_USD_millions’. You need to cover the following points as a part of the transformation process.

Task 3: Loading to CSV

Task 4: Querying the database table

Task 5: Logging progress
Logging needs to be done using the log_progress() funciton. This function will be called multiple times throughout the execution of this code and will be asked to add a log entry in a .txt file, etl_project_log.txt. The entry is supposed to be in the following format:
