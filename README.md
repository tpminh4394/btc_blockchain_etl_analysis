# btc_blockchain_analysis
In this project, I will show you how to store the entire btc blockchain ingformation in a relational database for 
on-chain data inspection and analysis. There is also a publicly available version of the btc blockchain in Google 
Bigquery dataset but they will charge you for every query. This is the main motivation why I do this project.

Hardware requirement is about 1T of storage. 

Summary of the procress:
**Step 1**: Set up a relational database. In my case I use postgres with DBeaver as a database management.

**Step 2:** Run a bitcoin core node. Which can be found here. https://bitcoincore.org/en/download/
This application will download the entire BTC blockchain to your computer (do not choose prune mode) in a compressed format
. The process would take 1-2 days and about 700GB of storage 

**Step 2:** 






