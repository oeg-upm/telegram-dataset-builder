# Telegram Dataset Builder

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.12773159.svg)](https://doi.org/10.5281/zenodo.12773159)

This project uses Telethon to build datasets of public Telegram groups. To do this, it needs the channel name or ID. The result is a json file with the channel information and several json files organised in folders containing the messages in batches.

## Requirements
The required packages and versions are the following:
```
Telethon==1.35.0
python-dotenv==1.0.1
```

## Telegram API credentials
The default configuration uses a ```telegram.env``` file in the root folder to load the credentials. This file must follow the next schema (note that the phone number must be with prefix):

```
PHONE_NUMBER= "+34..."
TELEGRAM_APP_ID = 9...
TELEGRAM_APP_HASH = "d..."
```

## How to gather groups messages?
To get all messages in some groups you can run ```dataset_creator.py``` and modify the next elements:

1. You have to modify the ```channel_names= ["foo", "bar"]``` to the channel names you want to extract. 
2. You can set a different ```BATCH_SIZE``` if you want.
3. If you put you telegram credentials in a different path, modify ```telegram_env_path```.
4. The ```output_chats_path``` is the folder were everythin is going to be stored. Both the channels chats and the channels info, it can be modified.