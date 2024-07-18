from tdb import TelethonHandler, Utils

# This script creates a dataset of the messages available in the different channel_names.
# By providing a folder, this script will create a subfolder for each channel and for each chat.
# Then, following a batch approach, it will download all the messages from oldest to newest.
# The channels info is also stored.

if __name__ == "__main__":
    # Define the names of the telegram channels to retrieve message from. (Can be names or ID-s)
    channel_names= ["foo", "bar"]

    BATCH_SIZE= 1000 # Maximum number of messages per output json file.
    telegram_env_path= "telegram.env" # Telegram API credentials environment file path.
    output_chats_path= "output_messages" # Directory to save all the batched messages.
    output_channel_info_path= f"{output_chats_path}/channels.json" # Path for the channels info output file.

    Utils.create_folder_if_not_exists(output_chats_path) # Create output folder if not existing

    # Initialize handler class and create a session. This must require to authenticate youserlf by introducing a code sent by telegram once executed.
    # Once the session is created you wont be ask for any number again.
    TG = TelethonHandler(telegram_env_path)
    TG.connect_client()

    # First step: Get information of all channels.
    output_channel_info= {}
    for channel_name in channel_names:
        output_channel_info[channel_name]= {}
        chats_ids= TG.get_channel_chats(channel_name)
        for chat_id in chats_ids:
            output_channel_info[channel_name][chat_id] = TG.get_chat_info(chat_id)

    # Dump channels info to file.
    Utils.save_dict(output_channel_info, output_channel_info_path)


    for channel_name in channel_names:
        print(f"Channel {channel_name} gathering.")
        Utils.create_folder_if_not_exists(f"{output_chats_path}/{channel_name}") # Create folder if not exists
        chats_ids= TG.get_channel_chats(channel_name)
        for chat_id in chats_ids:
            print(f"\tChat {chat_id} gathering.")
            Utils.create_folder_if_not_exists(f"{output_chats_path}/{channel_name}/{chat_id}") # Create folder if not exists
            _offset=0 # First message to retrieve
            n_batch= 0 # Batch counter
            while _offset is not None:
                n_batch += 1 # Batch counter incrementation
                messages, _offset= TG.get_n_messages(chat_id, n_messages=BATCH_SIZE, offset_id=_offset) # Get messagges per batch
                msgs= {}
                if messages and _offset: # If messages retrieved
                    for message in messages:
                        msgs= {**msgs, **Utils.format_message(message)} # Convert to dict the messages and aggregate in a single dict
                    Utils.save_dict(msgs, f"{output_chats_path}/{channel_name}/{chat_id}/batch_{n_batch}.json")
                    print(f"\t\tBatch {n_batch} dump.")
            print("\t\tChat dump finished.")
        print("\t\tChannel dump finished.")

    print("Extraction finished.")
