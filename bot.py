import asyncio
import logging
import os
import sqlite3

import asqlite
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import twitchio
from twitchio.ext import commands
from twitchio import eventsub

load_dotenv()

LOGGER: logging.Logger = logging.getLogger("Bot")

# Twitch credentials
CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
BOT_ID = os.getenv("TWITCH_BOT_ID")
OWNER_ID = os.getenv("TWITCH_BROADCASTER_ID")

# Google Sheets setup
GOOGLE_SHEETS_ID = os.getenv("GOOGLE_SHEETS_ID")
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("sheets_credentials.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(GOOGLE_SHEETS_ID).sheet1


def get_user_info(username: str, field_name: str) -> int | None:
    """Retrieve a numeric field (Tokens, Tickets) for a given user from Google Sheet."""
    try:
        records = sheet.get_all_records()
        for record in records:
            if record.get("Username", "").lower() == username.lower():
                return record.get(field_name, 0)
        return 0  # user not found - treat as 0
    except gspread.exceptions.SpreadsheetNotFound as e:
        LOGGER.error(f"Spreadsheet not found or inaccessible: {e}")
    except gspread.exceptions.APIError as e:
        LOGGER.error(f"Google Sheets API error: {e}")
    except Exception as e:  # what exception
        LOGGER.exception(f"Unexpected error in {get_user_info.__name__}")
    return None


class Bot(commands.Bot):
    def __init__(self, *, token_database: asqlite.Pool) -> None:
        self.token_database = token_database
        super().__init__(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            bot_id=BOT_ID,
            owner_id=OWNER_ID,
            prefix="!",
        )

    async def setup_hook(self) -> None:
        await self.add_component(MyComponent(self))

        subscription = eventsub.ChatMessageSubscription(broadcaster_user_id=OWNER_ID, user_id=BOT_ID)
        await self.subscribe_websocket(payload=subscription)

    async def add_token(self, token: str, refresh: str) -> twitchio.authentication.ValidateTokenPayload:
        resp: twitchio.authentication.ValidateTokenPayload = await super().add_token(token, refresh)

        query = """
        INSERT INTO tokens (user_id, token, refresh)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id)
        DO UPDATE SET
            token = excluded.token,
            refresh = excluded.refresh;
        """

        async with self.token_database.acquire() as connection:
            await connection.execute(query, (resp.user_id, token, refresh))

        LOGGER.info("Added token to the database for user: %s", resp.user_id)
        return resp

    async def load_tokens(self, path: str | None = None) -> None:
        # We don't need to call this manually, it is called in .login() from .start() internally...

        async with self.token_database.acquire() as connection:
            rows: list[sqlite3.Row] = await connection.fetchall("""SELECT * from tokens""")

        for row in rows:
            await self.add_token(row["token"], row["refresh"])

    async def setup_database(self) -> None:
        # Create our token table, if it doesn't exist..
        query = """CREATE TABLE IF NOT EXISTS tokens(user_id TEXT PRIMARY KEY, token TEXT NOT NULL, refresh TEXT NOT NULL)"""
        async with self.token_database.acquire() as connection:
            await connection.execute(query)

    async def event_ready(self) -> None:
        LOGGER.info("Successfully logged in as: %s", self.bot_id)


class MyComponent(commands.Component):
    def __init__(self, bot: Bot):
        self.bot = bot

    @commands.Component.listener()
    async def event_message(self, payload: twitchio.ChatMessage) -> None:
        print(f"[{payload.broadcaster.name}] - {payload.chatter.name}: {payload.text}")

    @commands.command(name="balance")
    async def check_balance(self, ctx: commands.Context):
        """Command that returns a number of custom tokens a user has.

        Retrieves the "Tokens" value associated with the user from a connected spreadsheet.
        Replies in chat with the token count or an error message.

        !balance
        """
        points = get_user_info(ctx.author.name, "Tokens")
        if points:
            await ctx.reply(f"{ctx.author.name}, you have {points} tokens.")
        else:
            await ctx.reply(f"{ctx.author.name}, there was an error checking your tokens.")

    @commands.command(name="tickets")
    async def check_tickets(self, ctx: commands.Context):
        """Command that returns a number of tickets a user has.

        Looks up the "Tickets" value in a spreadsheet by username.
        Replies in chat with the ticket count or an error message.

        !tickets
        """
        tickets = get_user_info(ctx.author.name, "Tickets")
        if tickets:
            await ctx.reply(f"{ctx.author.name}, you have {tickets} tickets.")
        else:
            await ctx.reply(f"{ctx.author.name}, there was an error checking your tickets.")


def main() -> None:
    twitchio.utils.setup_logging(level=logging.INFO)

    async def runner() -> None:
        async with asqlite.create_pool("tokens.db") as tdb, Bot(token_database=tdb) as bot:
            await bot.setup_database()
            await bot.start()

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        LOGGER.warning("Shutting down due to KeyboardInterrupt...")


if __name__ == "__main__":
    main()
