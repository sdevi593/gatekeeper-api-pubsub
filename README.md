# Gatekeeper-Api-Pubsub

## Requirements
- GCP Account
- Python 3.7.9 or more
- Visual Studio Code
- Postman
- Some Python libraries such as flask, datetime, googlecloud etc

## Installation
- Create your credentials key as owner, download and save it to your local, it will give you a json file, feel free to rename it (ex: key.json)
- Download Postman Application from https://www.postman.com/downloads/ and follow its tutorial https://www.guru99.com/postman-tutorial.html

## Gatekeeper Process
- Write your script for doing gatekeeper and push it to google bigquery, and since I'm using Windows, it's important to set your credential to make your codes working
  I set my credential by writing these on my scripts:
  import os
  os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/62895/Desktop/week4/key.json"
- Type bash pubsub.sh for creating topic and subscription on your Google Pub/Sub Account.
- Activate the API on your postman localhost by running the main.py file
- After getting your localhost address, go to postman application, create your postman account and go to post, copy your localhost there
  ![image](https://user-images.githubusercontent.com/59094767/127683441-0a95df20-efc5-40cb-b61c-2d960b7f518d.png)
- Before clicking the SEND button, run gatekeeper.py file, it results will be "Listeing to message on {your pubsub file project}. The gatekeeper.py file will initiate all process such as checking the columns name, data types, and values. If you want to modify the table, you can do it on BigQuery Table output later. 
- Next go to postman and tap the SEND button. At the listening state, google PubSub will listening to everything that published from postman, as long as it has the correct format.
- This process mainly consist of insert and delete command and following the BigQuery Rules:
  1. Insert : if the table isn't available yet, it will created one according to the fromat
  2. Alter : if the table is already exist in db, but our data isn't there yet, it will alter the table and add some relevant data
  3. Delete
- The output will be a BigQuery table with the message created from the postman application.
