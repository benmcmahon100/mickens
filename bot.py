import asyncio
import discord
import datetime
import os
import re
import sqlite3

from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from time import sleep
from typing import List

load_dotenv()

intents = discord.Intents.default()
intents.members = True
intents.message_content = True


class BotWrapper:
    def __init__(self, token: str, _executor: ThreadPoolExecutor) -> None:
        self.client = discord.Client(intents=intents)
        self.channels: List[discord.GuildChannel] = []
        self.tracked_messages: List[discord.Message] = {}
        (self.conn, self.cursor) = self.setup_db()

        self.executor = _executor

        self.register_listeners(self.client)
        self.client.run(token)

    @staticmethod
    def bootstrap_db(cur: sqlite3.Cursor) -> None:
        cur.execute("CREATE TABLE IF NOT EXISTS messages(message_id VARCHAR(255) UNIQUE, channel_id, jump_id, user_id, reaction_count)")

    @staticmethod
    def setup_db() -> (sqlite3.Connection, sqlite3.Cursor):
        conn: sqlite3.Connection = sqlite3.connect("test.db")
        cur: sqlite3.Cursor = conn.cursor()
        BotWrapper.bootstrap_db(cur)
        
        return (conn, cur) 
    
    @staticmethod
    # We don't know the type yet
    def get_emoji_name(emoji) -> str:
        return emoji.name if isinstance(emoji, discord.Emoji) or isinstance(emoji, discord.PartialEmoji) else emoji

    @staticmethod
    def calc_keks(reactions: List[discord.Reaction]) -> int:
        return sum([r.count for r in reactions if re.match("^kek", BotWrapper.get_emoji_name(r.emoji).lower()) is not None])

    async def update_tracked_messages_for_channel(self, channel: discord.TextChannel) -> None:
        if isinstance(channel, discord.TextChannel):
            # Get messages from the last three days
            three_days_ago = datetime.datetime.now() - datetime.timedelta(days=5)
            messages = [message async for message in channel.history(after=three_days_ago, oldest_first=True) if (self.calc_keks(message.reactions) if len(message.reactions) else 0) > 0]
            self.tracked_messages = {m.id : {"message": m, "reactions": m.reactions} for m in messages}
            for message in messages:
                print(f"{message.author} said {message.content} in {channel.name} and got {len(message.reactions)} reactions")
            
            message_tuples = [(f"{message.id}", f"{channel.id}", message.jump_url, message.author.id, self.calc_keks(message.reactions)) for message in messages]

            self.cursor.executemany("INSERT OR REPLACE INTO messages VALUES(?,?,?,?,?)", message_tuples)

    async def update_counts_for_channels(self):
        channels = self.client.get_all_channels()
        await asyncio.gather(*[self.update_tracked_messages_for_channel(channel) for channel in channels])
            

    async def setup_reload_polling(self):
        while True:
            await self.update_counts_for_channels()
            await asyncio.sleep(60*1)


    def register_listeners(self, client):
        @client.event
        async def on_ready():
            print(f'{client.user} has connected to Discord!')
            await self.update_counts_for_channels()
            asyncio.get_event_loop().create_task(self.setup_reload_polling())

        @client.event
        async def on_message(msg: discord.Message):
            if msg.content.startswith("!getNums"):
                await msg.channel.send("Here are your top 5 keks")
                for (message_id, channel_id, jump_url, author_id, reaction_count) in self.cursor.execute("SELECT * FROM messages ORDER BY reaction_count DESC LIMIT 5").fetchall():
                    channel = client.get_channel(int(channel_id))
                    author = discord.utils.get(client.get_all_members(), id=author_id)
                    full_message = await channel.fetch_message(message_id)
                    embedVar = discord.Embed(title=f"This gem got by {author.display_name}", color=0x00ff00)
                    embedVar.add_field(name="content", value=full_message.content)
                    embedVar.add_field(name="keks", value=reaction_count)
                    await msg.reply(embed=embedVar)



if __name__ == "__main__":
    _token = os.getenv("DISCORD_BOT_TOKEN")
    with ThreadPoolExecutor(max_workers=5) as _executor:
        wrapper = BotWrapper(_token, _executor)
