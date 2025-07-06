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
    """Retrieve a numeric field (Tokens, Tickets) for a given user from Google Sheet.

    Returns:
        int: value from the specified field, 0 if the user is not found.
        None: if a Google Sheet error occurs.
    """
    try:
        records = sheet.get_all_records()
    except gspread.exceptions.SpreadsheetNotFound as e:
        LOGGER.error(f"Spreadsheet not found or inaccessible: {e}")
        return None
    except gspread.exceptions.APIError as e:
        LOGGER.error(f"Google Sheets API error: {e}")
        return None
    except Exception as e:  # what exception
        LOGGER.exception(f"Unexpected error in {get_user_info.__name__}")
        return None

    for record in records:
        if record.get("Username", "").lower() == username.lower():
            return record.get(field_name, 0)
    return 0  # user not found - treat as 0


def update_user_fields(username: str, updates: dict[str, int]) -> bool:
    """Update numeric fields for a user with given values."""
    try:
        user_cell = sheet.find(username)
        if not user_cell:
            LOGGER.warning(f"Username {username} not found in sheet.")
            return False

        row = user_cell.row
        headers = sheet.row_values(1)  # get header row
    except Exception as e:  # :(
        LOGGER.error(f"Unexpected error: {e}")
        return False

    for field, new_value in updates.items():
        if field in headers:
            col = headers.index(field) + 1
            sheet.update_cell(row, col, new_value)  # try except?
    return True


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

    @commands.command(name="buy")
    async def buy_tickets(self, ctx: commands.Context, n: int):
        """Allows user to exchange their custom tokens for tickets.

        !buy <n>
        """
        username = ctx.author.name
        points = get_user_info(username, "Tokens")
        tickets = get_user_info(username, "Tickets")

        if points is None or tickets is None:
            await ctx.reply(f"{username}, failed to retrieve your balance.")
            return

        cost = 52000
        if int(points) < cost * n:
            await ctx.reply(f"{username}, you don't have enough tokens. Your balance is {points}, 1 ticket costs {cost} tokens.")
            return

        updated = update_user_fields(username, {
            "Tokens": int(points) - cost * n,
            "Tickets": int(tickets) + n
        })

        if updated:
            await ctx.reply(f"{username}, you bought {n} tickets. You now have {int(tickets) + n} tickets!")
        else:
            await ctx.reply(f"{username}, there was an error updating your data.")

    @commands.command(name="transfer")
    async def transfer_to_user(self, ctx: commands.Context, n: int, username: str):
        """Allow users to give their custom points to other users.

        !transfer <amount> <username>"""
        gifter = ctx.author.name
        receiver = username.lstrip("@").lower()

        if gifter.lower() == receiver:
            await ctx.reply("You can't transfer tokens to yourself.")
            return

        # Get balance for gifter and giftee
        gifter_points = get_user_info(gifter, "Tokens")
        receiver_points = get_user_info(receiver, "Tokens")

        if not gifter_points or not receiver_points:  # add receiver in table if null?
            await ctx.reply("Could not fetch balances.")
            return

        if n <= 0:
            await ctx.reply("Amount must be greater than 0.")
            return

        if int(gifter_points) < n:
            await ctx.reply(f"{gifter}, you don't have enough tokens.")
            return

        # Update fields for gifter and giftee
        updated = (update_user_fields(gifter, {"Tokens": int(gifter_points) - n})
                   and update_user_fields(receiver, {"Tokens": int(receiver_points) + n}))
        if updated:
            await ctx.send(f"{gifter} transferred {n} tokens to {receiver}.")
            LOGGER.info(f"Transfer: {gifter} -> {receiver}: {n} tokens.")
        else:
            await ctx.send("Error transferring tokens.")
            LOGGER.warning(f"Failed transfer: {gifter} -> {receiver}.")


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
