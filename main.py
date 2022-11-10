import pymysql
from dotenv import load_dotenv
import os
import discord
from discord.ext import commands
from discord.utils import get

pd.options.display.width = 0

# DISCORD BOT SCRAPPER
bot = commands.Bot(command_prefix="!", intents=intent)
TOKEN = os.getenv("DISCORD_TOKEN")
intent = discord.Intents.all()

load_dotenv() # Initialize environment variables from .env file

sql_endpoint = os.environ["RDS_ENDPOINT"] # Intitialize global variables
sql_user = os.environ["RDS_USER"]
sql_pass = os.environ["RDS_PASS"]

# SQL CONNECTION
db = pymysql.connections.Connection(host=sql_endpoint, user=sql_user, password=sql_pass)
cursor = db.cursor()
cursor.execute("SHOW DATABASES;")
print(cursor.fetchall())

project_dir = os.path.dirname(__file__)
global guild_dir

@bot.event
async def on_ready():
    print("I'M READY!")

def data_dir(guild_name):
    global guild_dir
    guild_dir = os.path.join(project_dir, guild_name)
    if not os.path.exists(guild_dir):
        os.mkdir(guild_dir)

async def parse_history(channel_id):
    channel = bot.get_channel(channel_id)
    print(f"PARSING HISTORY FOR {channel.name}")

    column_names = ['Datetime', 'Date', 'Time', 'Text Channel', 'Author ID', 'Author Name', 'Content']
    data = pd.DataFrame(columns=column_names)
    async for message in channel.history(limit=None):
        if not message.author.bot:
            msg = [
                    message.created_at,
                    message.created_at.date(),
                    message.created_at.time(),
                    message.channel,
                    message.author.id,
                    message.author.name,
                    message.content
                  ]
            data.loc[len(data)] = msg

    csv_path = os.path.join(guild_dir, str(message.channel.id) + '.csv')    # OUTPUT TO CSV
    data.to_csv(csv_path, index=False)

@bot.command()
async def scan(ctx):
    guild = ctx.guild.name
    data_dir(guild)

    start_timer = time.perf_counter()   # TIMING OF CHAT HISTORY PROCESS
    for guild in bot.guilds:
        for channel in guild.text_channels:
            await parse_history(channel.id)
    end_timer = time.perf_counter()

    await ctx.send(f'FINISHED IN {(end_timer-start_timer)/60} MINUTES!')

@bot.event
async def on_message(ctx):
    print(f"MESSAGE IN {ctx.channel}")

    await bot.process_commands(ctx) # Runs if the message was a command

    if not ctx.author.bot:
        global guild_dir # Using the channel id to get a directory
        guild = ctx.guild.name
        data_dir(guild)
        csv_path = os.path.join(guild_dir, str(ctx.channel.id) + '.csv')

        if os.path.exists(csv_path):    # READS CSV OR CREATES IT
            csv_df = pd.read_csv(csv_path)
        else:
            column_names = ['Date', 'Text Channel', 'Author ID', 'Author Name', 'Content']
            csv_df = pd.DataFrame(columns=column_names)

        msg = [
            ctx.created_at,
            ctx.created_at.date(),
            ctx.created_at.time(),
            ctx.channel,
            ctx.author.id,
            ctx.author.name,
            ctx.content
        ]

        csv_df.loc[len(csv_df)] = msg
        csv_df.to_csv(csv_path, index=False)

bot.run(TOKEN)
