# btc blockchain etl & analysis
In this project, I will show you how to store the entire btc blockchain information in a relational database for 
on-chain data inspection and analysis. There is also a publicly available version of the btc blockchain in Google 
Bigquery dataset but they will charge you for every query. This is the main motivation why I do this project.

Hardware requirement is about 1T of storage. 

Summary of the procress:

**Step 1**: Set up a relational database. In my case I use postgres with DBeaver as a database management.

**Step 2:** Run a bitcoin core node. Which can be found here. https://bitcoincore.org/en/download/
This application will download the entire BTC blockchain to your computer (do not choose prune mode) in a compressed format
. The process would take 1-2 days and about 700GB of storage 

**Step 3:** Parse btc data and dump them into your relational database. This process would take weeks and about 200GB of storage (We
have to choose carefully which information we need for storage and analysis or else the storage space will blow up. This process would take weeks to complete. 

**Step 4:** Post Processing. After getting the raw data. We should convert them into a set dim fact tables for easier analysis later
down the road. Some suggestion: A dim table of entity (as one entity may have many address). A snapshot fact table capturing monthly balance, value transaction, number of transaction of entity. 

**Step 5:** Now we are ready for Dashboard building/on chain analytics 

By now I am still parsing data in part 3. While waiting for complete parsing. We can prepare some table transfromation for part 4 and start doing some simple analysis for part 5







