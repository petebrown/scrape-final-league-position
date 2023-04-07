import pandas as pd
import requests
import logging

def construct_url(division, year):
    url = f"https://www.11v11.com/league-tables/{division}/{year}/"
    return url

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
}

df = pd.read_csv("https://raw.githubusercontent.com/petebrown/update-results/main/data/results_df.csv", parse_dates=["game_date"])

final_games = df[(df.game_type == 'League') & (~df.competition.str.contains("Play-Off")) & (df.competition != "National League")].groupby("season").agg({"game_date": "max"}).sort_values("game_date", ascending=True).reset_index()

df = df[df.game_date.isin(final_games.game_date)].copy()

df['new_league_name'] = df.competition.str.lower().str.replace(" ", "-", regex=False).str.replace("(", "", regex=False).str.replace(")", "", regex=False)

df['year'] = df.game_date.dt.year

df['table_url'] = df.apply(lambda x: construct_url(x.new_league_name, x.year), axis=1)

table_urls = df.table_url.to_list()

tables_df = pd.DataFrame()
for url in table_urls:
    try:    
        r = requests.get(url, headers = headers)

        doc = r.text
        table = pd.read_html(doc)[0]
        df = pd.DataFrame(table)
        df = df[['Pos', 'Team', 'Pld', 'W', 'D', 'L', 'GF', 'GA', 'Pts']]
        df['index_no'] = df.index + 1
        df['url'] = url
        tables_df = pd.concat([tables_df, df])
    except Exception as e:
        logging.basicConfig(filename='error.log', filemode="w", encoding='utf-8', format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.DEBUG)
        logging.warning('Failed trying to scrape %s', url)
        logging.error('Encountered an issue: %s', e)

try:
    tables_df.Pos = tables_df.Pos.astype(str).str.replace("doRowNumer();", "", regex=False)
except AttributeError:
    pass

tables_df.to_csv("./data/final-positions.csv", index = False)