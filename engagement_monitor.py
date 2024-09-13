from tdb import TelethonHandler, Utils
import os, time
from datetime import datetime

# This script monitors the messages sent in a series of telegram groups to note the evolution of their metrics and values.

FORCE_COLD_START= False # Force cold start allways
BATCH_SIZE= 1000 # Maximum number of messages per output json file.
TRACKER_WINDOW= 2592000 # Time during which channel messages are monitored (in seconds).
TRACKER_TIMER= 300 # Periodicity of message monitoring.

home_path = "."# Variable to set a full path to all files and folders. Needed to create daemons
session_id= f"{home_path}/session0.session"

# Define the names of the telegram channels to retrieve message from. (Can be names or ID-s)
channel_names= ["foo", "bar"]

telegram_env_path= f"{home_path}/telegram.env" # Telegram API credentials environment file path.
output_chats= f"{home_path}/monitoring" # Directory to save all the batched messages.
output_channel_info= f"{output_chats}/channels.json" # Path for the channels info output file.

output_runtime= f"{output_chats}/runtime" # Directory to save all the batched messages.
output_chat_id2channel_name= f"{output_runtime}/chat_id2channel_name.json" # Path for the chat_id2channel_name output file.
output_chat_id2savepath= f"{output_runtime}/batched_savepaths.json" # Path for the savepaths mapping
output_chat_id2offset= f"{output_runtime}/offsets.json" # Path to the chat offset mapping

output_tracker= f"{output_runtime}/tracking.json" # Path for the tracking messages file.

chat_id2channel_name= {} # This will be important to maintain the subfolders structure on the monitor
chat_id2savepath= {} # chat_id: path to file to append message. Important to mantain batched file size.
chat_id2offset= {} # Mapping between chats and last message id. Important to detect new messages.

def generate_message_id(chat_id, message_id, published_timestamp, tracked_timestamp) -> str:
    n_entity= round((tracked_timestamp-published_timestamp)/TRACKER_TIMER, 1)
    return f"{chat_id}_{message_id}_{n_entity}"

def is_message_different(old_message, new_message) -> bool:
    """Check if two messages are different

    Args:
        old_message (dict): Old message.
        new_message (dict): New Message.

    Returns:
        bool: True if the messages are different.
    """
    key_exceptions= ["tracker_retrieved"]
    old_message_keys= [*old_message]
    new_message_keys= [*new_message]

    for old_key, old_value in old_message.items():
        if old_key in key_exceptions: continue # Key not compared
        if old_key not in new_message_keys: return True
        elif str(old_value) != str(new_message[old_key]): return True

    for new_key, new_value in new_message.items():
        if new_key in key_exceptions: continue # Key not compared
        if new_key not in old_message_keys: return True
        elif str(new_value) != str(old_message[new_key]): return True

    return False

def save_batched(chat_id, new_messages):
    old_messages= {}
    chat_id= str(chat_id)
    total_len= len(new_messages.keys())
    if os.path.isfile(chat_id2savepath[chat_id]):
        old_messages= Utils.load_dict(chat_id2savepath[chat_id])
        total_len += len(old_messages.keys())

    if total_len <= BATCH_SIZE:
        # It fits in the same file
        old_messages.update(new_messages)
        Utils.save_dict(old_messages, chat_id2savepath[chat_id])
        if total_len == BATCH_SIZE:
            # It fits but savepath must be updated for the next one
            n_batch= int(chat_id2savepath[chat_id].split('/batch_')[-1].split('.')[0])+1
            chat_id2savepath[chat_id]= f"{chat_id2savepath[chat_id].split('/batch_')[0]}/batch_{n_batch}.json"
    elif total_len > BATCH_SIZE:
        # does not fit

        # Fill the current file 
        fill_len= BATCH_SIZE - len(old_messages.keys()) # Space available in the batched file        
        if fill_len <0: # This can only happen when batch size is changed for a restart
            fill_len= BATCH_SIZE

        keys = list(new_messages.keys())
        first_part = {key: new_messages[key] for key in keys[:fill_len]} # Get enought messages to fill the file
        old_messages.update(first_part) # Update dict with new messages
        Utils.save_dict(old_messages, chat_id2savepath[chat_id]) # Dump dict to file
        
        # Update savepath
        n_batch= int(chat_id2savepath[chat_id].split('/batch_')[-1].split('.')[0])+1
        chat_id2savepath[chat_id]= f"{chat_id2savepath[chat_id].split('/batch_')[0]}/batch_{n_batch}.json"

        # Remaining messages and number of files to create
        remaining_part = {key: new_messages[key] for key in keys[fill_len:]}
        n_files= len(list(remaining_part.keys()))/BATCH_SIZE

        # Fill n files
        while n_files > 0:
            remaining_keys= [*remaining_part]

            # Get messages to save in this file
            batch_messages = {key: remaining_part[key] for key in remaining_keys[:BATCH_SIZE]}
            Utils.save_dict(batch_messages, chat_id2savepath[chat_id])

            # Update remaining messages
            remaining_part = {key: remaining_part[key] for key in remaining_keys[BATCH_SIZE:]}
            
            # Update savepath
            n_batch= int(chat_id2savepath[chat_id].split('/batch_')[-1].split('.')[0])+1
            chat_id2savepath[chat_id]= f"{chat_id2savepath[chat_id].split('/batch_')[0]}/batch_{n_batch}.json"

            n_files -= 1 # Continue with next file        
    
    Utils.save_dict(chat_id2savepath, output_chat_id2savepath)

if __name__ == "__main__":
    # ********* #
    Utils.create_folder_if_not_exists(output_chats) # Create output folder if not existing
    Utils.create_folder_if_not_exists(output_runtime) # Create output folder if not existing

    # Initialize handler class and create a session. This must require to authenticate youserlf by introducing a code sent by telegram once executed.
    # Once the session is created you wont be ask for any number again.
    TG = TelethonHandler(telegram_env_path)
    TG.connect_client(session_id=session_id)

    # First step: Get information of all channels.
    channel_info= {}
    chats_ids= []
    for channel_name in channel_names:
        channel_info[channel_name]= {}
        chat_ids= TG.get_channel_chats(channel_name)
        chats_ids.extend(chat_ids)
        for chat_id in chat_ids:
            chat_id2channel_name[chat_id]= channel_name
            channel_info[channel_name][chat_id] = TG.get_chat_info(chat_id)

    # Dump channels info to file.
    Utils.save_dict(channel_info, output_channel_info)
    Utils.save_dict(chat_id2channel_name, output_chat_id2channel_name)

    if not os.path.isfile(output_tracker) or FORCE_COLD_START:
        print("Cold start. Genearting json files.")
        tracking_messages= {}
        Utils.save_dict(tracking_messages, output_tracker)
        print("Tracker file created.")

        for chat_id in chats_ids:
            last_message, last_message_id= TG.get_last_message(chat_id)
            if last_message_id is None:
                print("No message found.")
                continue
            
            chat_id2savepath[chat_id]= f"{output_chats}/{chat_id2channel_name[chat_id]}"
            Utils.create_folder_if_not_exists(chat_id2savepath[chat_id])
            chat_id2savepath[chat_id]= f"{chat_id2savepath[chat_id]}/{chat_id}"
            Utils.create_folder_if_not_exists(chat_id2savepath[chat_id])
            chat_id2savepath[chat_id]= f"{chat_id2savepath[chat_id]}/batch_1.json"

            chat_id2offset[chat_id]= last_message_id

        Utils.save_dict(chat_id2savepath, output_chat_id2savepath)
        print("Savepaths file created.")

        Utils.save_dict(chat_id2offset, output_chat_id2offset)
        print("Offset file created.")

        print("All JSONs created.")

    # Resume tracking or continue after cold start
    if os.path.isfile(output_tracker):
        print("Warm start. Tracking from json files.")

        chat_id2savepath= Utils.load_dict(output_chat_id2savepath)
        chat_id2offset= Utils.load_dict(output_chat_id2offset)
        chat_id2channel_name= Utils.load_dict(output_chat_id2channel_name)

        chat_ids= [*chat_id2channel_name]

        tracking_messages= Utils.load_dict(output_tracker)

        # channel_info= Utils.load_dict(output_channel_info)

        print("All JSONs loaded.")

        while True:
            # Monitor tracked messages
            print("Monitoring tracked messages.")
            auxiliar_tracker_dump_flag= False # Flag to force a tracker dump if did not happen (default dump only work when new messages are detected)
            different_messages= {} # Dict to store the different messages per chat_id. This is to dump per chat_id instead of individually.
            for message_key, message in tracking_messages.copy().items():
                now_time= datetime.now() # The moment when the messages are monitored

                if now_time.timestamp() > datetime.fromisoformat(message.get("date")).timestamp() + TRACKER_WINDOW: 
                    del tracking_messages[message_key] # Tracking time expired
                    auxiliar_tracker_dump_flag= True
                    continue
                
                updated_message, updated_message_id= TG.get_a_message(message.get("channel_id"), message.get("id"))
                if updated_message_id is None:
                    del tracking_messages[message_key] # Message removed
                    auxiliar_tracker_dump_flag= True
                    continue

                updated_message= Utils.format_message(updated_message,tracker_retrieved=now_time.isoformat())
                updated_message_key= [*updated_message][0]
                
                if not is_message_different(message, updated_message[updated_message_key]): continue # If the message did not change continue 

                if updated_message[updated_message_key].get("channel_id") not in different_messages: 
                    different_messages[updated_message[updated_message_key].get("channel_id")]= {}

                tracking_messages[message_key].update(updated_message[updated_message_key]) # Update tracking file values
                Utils.save_dict(tracking_messages, output_tracker) # Update json file
                auxiliar_tracker_dump_flag= False # Tracker dumped

                updated_message_key= generate_message_id(updated_message[updated_message_key].get("channel_id"), updated_message[updated_message_key].get("id"), 
                                                         datetime.fromisoformat(updated_message[updated_message_key].get("date")).timestamp(), 
                                                         now_time.timestamp()) # Generate a unique id for the instance of this message

                updated_message[updated_message_key]= updated_message.pop([*updated_message][0]) # Update the key

                different_messages[updated_message[updated_message_key].get("channel_id")]= {**different_messages[updated_message[updated_message_key].get("channel_id")], **updated_message} 

            # Dump updated messages to JSON file.
            if different_messages:
                print("Updated messages dump to file.")
                for chat_id, messages in different_messages.items():
                    save_batched(chat_id, messages)

            if auxiliar_tracker_dump_flag: 
                Utils.save_dict(tracking_messages, output_tracker) # Update json file

            print("Monitoring finished.")

            # Get new messages
            print("Looking for new messages.")
            auxiliar_offset_dump_flag= False # Flag to specify when to dump offset file
            for chat_id in chat_ids:
                messages, offset_id= TG.get_n_messages(chat_id, offset_id=chat_id2offset[chat_id])

                if offset_id is None: continue # No new messages for this chat
                else: auxiliar_offset_dump_flag= True # New offset

                chat_id2offset[chat_id]= offset_id # update offset
                Utils.save_dict(chat_id2offset, output_chat_id2offset)

                now_time= datetime.now() # The moment the messages where retrieved

                new_messages= {}
                for message in messages:
                    message_date= message.date

                    # If the "new" message was retrieved more than 10 hours after the message was sent dont track it (the more recent the more detail in the evolution)
                    if now_time.timestamp() > message_date.timestamp()+(10*60*60): continue

                    message_id= message.id
                    message= Utils.format_message(message, tracker_retrieved=now_time.isoformat())

                    tracking_messages= {**tracking_messages, **message} # Convert to dict the messages and aggregate in a single dict

                    message_key= generate_message_id(chat_id, message_id, message_date.timestamp(), now_time.timestamp())

                    message[message_key]= message.pop([*message][0])
                    new_messages= {**new_messages, **message}

                save_batched(chat_id, new_messages)
                print(f"New messages for chat {chat_id}: {len([*new_messages])}")

                Utils.save_dict(tracking_messages, output_tracker) # Update tracking file
            print("New messages search finished.")

            if auxiliar_offset_dump_flag:
                Utils.save_dict(chat_id2offset, output_chat_id2offset)
                print("Offset file updated.")

            time.sleep(TRACKER_TIMER) # Wait until next loop.
        
        print("Monitoring finished.")
    
    print("Script finished.")