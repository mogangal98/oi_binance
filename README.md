This code fetches open interest data from Binance at 1-minute intervals and 
saves it into a MySQL database. To ensure the data is as close as possible
to the start of each minute, it uses multithreading to send multiple 
API requests simultaneously for over 100 trading pairs, improving speed and efficiency. 
The collected data is used in other projects.

Note:
This is a simplified version of the original code provided for demonstration purposes on GitHub. 
Sensitive information, such as database credentials and some 
other implementation details, has been removed or modified.