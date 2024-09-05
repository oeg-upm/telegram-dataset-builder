import os
import json
from telethon.sync import TelegramClient
from telethon.tl.functions.channels import GetFullChannelRequest

from dotenv import load_dotenv

class TelethonHandler:
    def __init__(self, env_file) -> None:
        """Creates the Telethon Handler class. Credentials needed. 

        Args:
            env_file (str): Path to telethon environment file.
        """
        load_dotenv(env_file)

        self.PHONE_NUMBER = os.getenv('PHONE_NUMBER')
        self.TELEGRAM_APP_ID = os.getenv('TELEGRAM_APP_ID')
        self.TELEGRAM_APP_HASH = os.getenv('TELEGRAM_APP_HASH')


    def connect_client(self, session_id='session0'):
        """Create the session to query the telegram API:

        Args:
            session_id (str, optional): Name of the session file. Defaults to 'session0'.
        """
        self.client = TelegramClient(session_id, int(self.TELEGRAM_APP_ID) , self.TELEGRAM_APP_HASH)
        self.client.connect()

        if not self.client.is_user_authorized():
            self.client.send_code_request(self.PHONE_NUMBER)
            me = self.client.sign_in(self.PHONE_NUMBER, input('Enter Telegram code: '))

        assert self.client.start()


    def get_a_message(self, chat_id:int, message_id:int)-> tuple:
        """Gather a menssage in a chat.

        Args:
            chat_id (int): ID of the chat to get message from.
            message_id (int): ID of the message in the chat.
        Returns:
            tuple(list,int): Message , message id.
        """
        async def async_get_a_messages(self, chat_id, message_id):
            message = await self.client.get_messages(chat_id, ids=message_id)

            if not message: # If no message found return None
                return None, None
            else:
                return message, message.id

        return self.client.loop.run_until_complete(async_get_a_messages(self, int(chat_id), int(message_id)))

    def get_last_message(self, chat_id:int)-> tuple:
        """Gather last menssage in a chat.

        Args:
            chat_id (int): ID of the chat to get message from.
        Returns:
            tuple(list,int): Message , message id.
        """
        async def async_get_last_messages(self, chat_id):
            message = await self.client.get_messages(chat_id, offset_id=0, limit=1, reverse=False) #Reverse False gets messages from newest to oldest
            message = message[0]
            if not message: # If no message found return None
                return None, None
            else:
                return message, message.id

        return self.client.loop.run_until_complete(async_get_last_messages(self, int(chat_id)))

    def get_n_messages(self, chat_id:int, n_messages=None, offset_id=0)-> tuple:
        """Gather menssages in a chat.

        Args:
            chat_id (int): ID of the chat to get messages from
            n_messages (int, optional): Maximum number of messages to retrieve. Defaults to None.
            offset_id (_type_, optional): ID of the initial message (NOT INCLUDED), starting point of data gathering. Defaults to 0.

        Returns:
            tuple(list,int): Messages list, last message id.
        """
        async def async_get_n_messages(self, chat_id, n_messages, offset_id):
            all_messages = []
            
            limit = 100  # Maximum number of messages per request (adjust as needed)
            while True:
                messages = await self.client.get_messages(chat_id, offset_id=offset_id, limit=limit, reverse=True) #Reverse True gets messages from oldest to newest
                if not messages:
                    break  # If there are no more messages, exit the loop
                
                if n_messages: # If maximum number of messages requested
                    if len(all_messages) == n_messages:
                        break
                    elif len(all_messages)+len(messages) > n_messages:
                        _n_messages= len(messages)-(len(all_messages)+len(messages)-n_messages)
                        messages= messages[:_n_messages]
                        offset_id = messages[-1].id
                        all_messages.extend(messages)
                        break
                
                all_messages.extend(messages)
                offset_id = messages[-1].id  # Update the offset_id for the next request
            
            if not all_messages: # If no messages found return None
                return None, None
            else:
                return all_messages, offset_id

        return self.client.loop.run_until_complete(async_get_n_messages(self, int(chat_id), n_messages, int(offset_id)))

    def get_channel_chats(self, channel_name:str)-> list:
        """Get chat ids from channel.

        Args:
            channel_name (str): Telegram channel name.

        Returns:
            list: List of chat ids in this channel.
        """
        async def async_get_channel_chats(self, channel_name):
            channel_entity = await self.client.get_entity(channel_name)
            channel = await self.client(GetFullChannelRequest(channel=channel_entity))
            chat_ids= [chat.id for chat in channel.chats]
            
            return chat_ids
        
        return self.client.loop.run_until_complete(async_get_channel_chats(self, channel_name))

    def get_chat_info(self, chat_id:int):
        """Gets chat info in dictionary format.

        Args:
            chat_id (int): Chat id to retrieve information
        """
        async def async_get_chat_info(self, chat_id):
            chat= await self.client.get_entity(chat_id)
            chat_info= {
                "id": chat.id,
                "created": chat.date.isoformat(),
                "title": chat.title,
                "username": chat.username
            }

            channel= await self.client(GetFullChannelRequest(channel=chat_id))
            if chat_id in [_chat.id for _chat in channel.chats]:
                chat_info["about"]= channel.full_chat.about
                chat_info["participants_count"]= channel.full_chat.participants_count

            return chat_info
        return self.client.loop.run_until_complete(async_get_chat_info(self, int(chat_id)))


class Utils:
    @staticmethod
    def save_dict(_dict:dict, path:str) -> bool:
        """Dumps dict to a json file..

        Args:
            _dict (dict): Dict to be dumped.
            path (str): Path to dump.

        Returns:
            bool: True if the file is dumped.
        """
        with open(path, "w") as f:
            f.write(json.dumps(_dict))
        print(f"File dumped to {path}")
        return True
    
    @staticmethod
    def load_dict(path:str) -> json:
        """Load from a json file.

        Args:
            path (str): Path of the json file.

        Returns:
            json: Loaded json.
        """
        with open(path, "r") as f:
            return json.load(f)

    @staticmethod
    def create_folder_if_not_exists(folder_path:str) -> bool:
        """Create a folder if it is not in the system.

        Args:
            folder_path (str): Desired folder path to create.

        Returns:
            bool: True if the folder is created. False if it already exists.
        """
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print(f"Folder '{folder_path}' created.")
            return True
        else:
            print(f"Folder '{folder_path}' already exists.")
            return False

    @staticmethod
    def format_message(message, **kwargs) -> json:
        """Convert Telethon message to dict.

        Args:
            message (_type_): Telethon Message.
            **kwargs: Any value in the dict can be replaced manually by passing in a value. New attributes can also be added.
        Returns:
            dict: dictionary of the message relevant properties.
        """
        msg = message.__dict__.copy()

        if kwargs.get("msg_key") is None: 
            msg_key= f'{msg["_chat_peer"].channel_id}_{msg["id"]}' # Default value of dict key if no other msg_key
        else:
            msg_key= kwargs.get("msg_key")

        if msg["media"]:
            content_type= type(msg["media"]).__name__
        else:
            content_type= None
        if content_type == "MessageMediaWebPage":
            _msg_media_= {}
            if "url" in  msg["media"].webpage.__dict__:
                _msg_media_["url"]= msg["media"].webpage.url
            else:
                _msg_media_["url"]= None

            if "site_name" in  msg["media"].webpage.__dict__:
                _msg_media_["site_name"]= msg["media"].webpage.site_name
            else:
                _msg_media_["site_name"]= None

            if "title" in  msg["media"].webpage.__dict__:
                _msg_media_["title"]= msg["media"].webpage.title
            else:
                _msg_media_["title"]= None

            if "description" in  msg["media"].webpage.__dict__:
                _msg_media_["description"]= msg["media"].webpage.description
            else:
                _msg_media_["description"]= None
        elif content_type is not None:
            _msg_media_= content_type # This could be extended to store other elements
        else: 
            _msg_media_ = None
        msg["media_type"]= content_type
        msg["media"]= _msg_media_

        if msg["replies"] is not None:
            msg["replies"]= msg["replies"].replies
        else:
            msg["replies"]= None

        if msg["reply_to"] is not None:
            msg["reply_to"]= msg["reply_to"].reply_to_msg_id
        else:
            msg["reply_to"]= None

        _aux_reactions= {}
        if msg["reactions"] is not None and msg["reactions"].results is not None:
            for reaction in msg["reactions"].results:    
                if not type(reaction.reaction).__name__ in _aux_reactions:
                    _aux_reactions[type(reaction.reaction).__name__]= {}
                
                if type(reaction.reaction).__name__ == "ReactionCustomEmoji":
                    _aux_reactions[type(reaction.reaction).__name__][reaction.reaction.document_id]= reaction.reaction.__dict__.copy()
                    _aux_reactions[type(reaction.reaction).__name__][reaction.reaction.document_id]["count"]= reaction.count
                else:
                    _aux_reactions[type(reaction.reaction).__name__][reaction.reaction.emoticon]= reaction.reaction.__dict__.copy()
                    _aux_reactions[type(reaction.reaction).__name__][reaction.reaction.emoticon]["count"]= reaction.count
                
        msg["reactions"]= _aux_reactions
        msg["channel_id"]= msg["_chat_peer"].channel_id

        if msg["fwd_from"]:
            _fwd_from_ = {}

            if msg["_forward"]._chat:
                _chat_= msg["_forward"]._chat.__dict__.keys()
                if "date" in _chat_: _fwd_from_["channel_created"]= msg["_forward"]._chat.date.isoformat()
                if "title" in _chat_: _fwd_from_["channel_title"]=  msg["_forward"]._chat.title

            _fwd_from_dict_ = msg["fwd_from"].__dict__
            if "channel_post" in _fwd_from_dict_: _fwd_from_["forwarded_message_id"]= msg["fwd_from"].channel_post
            if "date" in _fwd_from_dict_: _fwd_from_["forwarded_message_date"]= msg["fwd_from"].date.isoformat()
            if "sender_id" in _fwd_from_dict_: _fwd_from_["forwarded_sender_id"]= msg["_forward"].sender_id
            if "from_id" in _fwd_from_dict_: 
                if type(msg["fwd_from"].from_id).__name__ == 'PeerUser':
                    _fwd_from_["user_id"]= msg["fwd_from"].from_id.user_id  # Forwarded from user
                elif type(msg["fwd_from"].from_id).__name__ == 'PeerChannel':
                    _fwd_from_["channel_id"]= msg["fwd_from"].from_id.channel_id # Forwarded from channel
                elif not msg["fwd_from"].from_id:
                    pass # Is None
                else:
                    print("Unexpected forward type.")
            if "is_private" in _fwd_from_dict_: _fwd_from_["channel_is_private"]= msg["_forward"].is_private
            
            if msg["_forward"]._chat and not type(msg["_forward"]._chat).__name__ == 'ChannelForbidden':
                msg["channel_name"]= msg["_forward"]._chat.username
            if not msg["_forward"]._chat:
                msg["channel_name"]=  None
            else:
                msg["channel_name"]= "ChannelForbidden"
            msg["fwd_from"]= _fwd_from_

        del msg["_client"]
        del msg["_text"]
        del msg["_file"]
        del msg["_reply_message"]
        del msg["_buttons"]
        del msg["_buttons_flat"]
        del msg["_buttons_count"]
        del msg["_via_bot"]
        del msg["_via_input_bot"]
        del msg["_action_entities"]
        del msg["_linked_chat"]
        del msg["_chat_peer"]
        del msg["_input_chat"]
        del msg["_chat"]
        del msg["_broadcast"]
        del msg["_sender"]
        del msg["_input_sender"]
        del msg["_forward"]
        del msg["_reply_to_chat"]
        del msg["_reply_to_sender"]
        del msg["entities"]
        del msg["silent"]
        del msg["out"]
        del msg["mentioned"]
        del msg["media_unread"]
        del msg["post"]
        del msg["from_scheduled"]
        del msg["legacy"]
        del msg["edit_hide"]
        del msg["noforwards"]
        del msg["invert_media"]
        del msg["offline"]
        del msg["from_id"]
        del msg["from_boosts_applied"]
        del msg["saved_peer_id"]
        del msg["via_bot_id"]
        del msg["via_business_bot_id"]
        del msg["reply_markup"]
        del msg["grouped_id"]
        del msg["peer_id"]
        del msg["restriction_reason"]
        del msg["ttl_period"]
        del msg["quick_reply_shortcut_id"]
        del msg["action"]
        del msg["post_author"]

        msg["date"]= msg["date"].isoformat() # Convert date to serialize in json
        if msg["edit_date"]:
            msg["edit_date"]= msg["edit_date"].isoformat() # Convert date to serialize in json

        # Update the dict with manually set attributes
        if "msg_key" in kwargs: del kwargs["msg_key"] # The key of the dict is not added to the dict by default

        if kwargs: msg.update(kwargs) # If any element in kwargs update the msg with its values
        
        return {msg_key: msg}
