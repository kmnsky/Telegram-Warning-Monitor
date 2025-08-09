from telethon.sync import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.errors import FloodWaitError, ChatWriteForbiddenError
import asyncio
import pandas as pd
import csv
import os
import pandas as pd
import numpy as np
import json
from datetime import datetime

from config import api_id, api_hash, phone, source_group, your_group


async def main():
    async with TelegramClient('session_name', api_id, api_hash) as client:
        try:
            # Log in
            if not await client.is_user_authorized():
                await client.start(phone=phone)
                print("Please enter the code sent to your Telegram app:")
            
            # Join the source group (optional if already a member)
            try:
                await client(JoinChannelRequest(source_group))
                print(f"Joined source group: {source_group}")
            except Exception as e:
                print(f"Error joining source group (may already be a member): {e}")
            
            # Step 1: Receive messages from the source group
            try:
                with open('last_message_id.json', 'r') as f:
                    last_message_id = json.load(f).get('last_id', 0)
            except FileNotFoundError:
                last_message_id = 0
            
            new_messages = []
            async for message in client.iter_messages(source_group, limit=20, min_id=last_message_id):
                sender = await message.get_sender()
                sender_name = sender.username if sender and sender.username else "Unknown"
                message_data = {
                    'message_id': message.id,
                    'sender': sender_name,
                    'date': str(message.date),
                    'text': message.text or ""
                }
                new_messages.append(message_data)
                last_message_id = max(last_message_id, message.id)
            
            # Save last message ID
            with open('last_message_id.json', 'w') as f:
                json.dump({'last_id': last_message_id}, f)
            
            # Step 2: Write messages to a file
            file_exists = os.path.isfile('telegram_messages.csv')
            with open('telegram_messages.csv', 'a', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['message_id', 'sender', 'date', 'text'])
                if not file_exists:
                    writer.writeheader()
                writer.writerows(new_messages)
            print(f"Appended {len(new_messages)} messages to telegram_messages.csv")
            
            # Step 3: Process received messages
            data = pd.read_csv('telegram_messages.csv')
            data.columns = data.columns.str.strip().str.lower().str.replace(' ', '_')
            data.drop_duplicates(subset='message_id', inplace=True)
            data['message'] = data['text'].str.replace(r'\s+', ' ', regex=True).str.strip()
            data['message'] = data['message'].str.replace(r'[^a-zA-Zа-яА-ЯёЁґҐєЄіІїЇ ]', '', regex=True)
            data['message'] = data['message'].str.lower()
            data['warning'] = False
            data['attention'] = False
            data.dropna(subset=['text'], inplace=True)
            data.loc[data['message'].str.contains(
                '\шевченківсь|соломянс|голосіїв|святошин|нивки|борщаг|коцюбинськ|виноградар|пуща'), 'warning'] = True
            data.loc[data['message'].str.contains(
                r'\bбалістик|пуск|швидкісна|кинджал|іскандер|калібр\B'), 'warning'] = True
            data.loc[data['message'].str.contains(r'\bкиїв\b'), 'attention'] = True
            data.loc[data['message'].str.contains(
                'одес|одещ|чорного моря|миколаївщина|кіровоградщин|крим|харків'), ['warning', 'attention']] = False
            data.loc[data['message'].str.contains(
                'сумщин|загроза балістики зі сходу'), 'attention'] = True
            data.loc[data['message'].str.contains(
                r'\bу напрямку києва|входження крилатих ракет|загроза балістики зі сходу\b'), 'warning'] = True
            data.loc[data['message'].str.contains(
                'дорозвідка|⚡️|🔱|💬'), ['attention', 'warning']] = False
            data['information'] = data['text'].str.contains(r'⚡️|🔱|💬', regex=True, na=False)
            data.loc[data['message'].str.contains(r'\bмігк\b'), 'information'] = True
            data['alarm_reset'] = data['text'].str.contains(r'⚪️ |Відбій', regex=True, na=False)
            data.sort_values(by='date', inplace=True)
            data_send = data.query(
                "warning==True & information==False & alarm_reset==False").copy()
            
            # Step 4: Check if such messages have already been sent to my group
            sent_message_ids = set()
            sent_messages_file = 'sent_messages.csv'
            if os.path.isfile(sent_messages_file):
                sent_df = pd.read_csv(sent_messages_file)
                sent_message_ids = set(sent_df['original_message_id'].astype(int))
            
            # Step 5: Send new messages to my group
            new_sent_messages = []
            for _, row in data_send.iterrows():
                if row['message_id'] not in sent_message_ids:
                    try:
                        message = await client.send_message(your_group, row['text'])
                        print(f"Sent message ID {message.id} to your group: {row['text']}")
                        new_sent_messages.append({
                            'original_message_id': row['message_id'],
                            'sent_message_id': message.id,
                            'sent_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z'),
                            'text': row['text']
                        })
                        sent_message_ids.add(row['message_id'])
                        await asyncio.sleep(5)  # Delay to avoid rate limits
                    except ChatWriteForbiddenError:
                        print(f"Error: Cannot send message to {your_group}. Check group permissions.")
                        break
                    except FloodWaitError as e:
                        print(f"FloodWaitError: Must wait {e.seconds} seconds")
                        await asyncio.sleep(e.seconds)
                        continue
            
            # Save sent messages
            if new_sent_messages:
                file_exists = os.path.isfile(sent_messages_file)
                with open(sent_messages_file, 'a', encoding='utf-8', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=['original_message_id', 'sent_message_id', 'sent_date', 'text'])
                    if not file_exists:
                        writer.writeheader()
                    writer.writerows(new_sent_messages)
                print(f"Recorded {len(new_sent_messages)} sent messages to {sent_messages_file}")
        
        except Exception as e:
            print(f"Error: {e}")


# Run periodically every 1 minute
async def run_periodically():
    while True:
        print(f"Processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}")
        await main()
        print("Waiting 1 minute before next run...")
        await asyncio.sleep(60)

# For Jupyter Notebook
await run_periodically()
