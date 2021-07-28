import requests
import pandas as pd
import numpy as np
import sqlite3
import seaborn as sns
import matplotlib.pyplot as plt

sns.set()

con = sqlite3.connect('fpl21.db')
con.row_factory = sqlite3.Row
cur = con.cursor()

def init_db():
    cur.execute('CREATE TABLE IF NOT EXISTS managers(id INTEGER PRIMARY KEY, club VARCHAR, name VARCHAR)')   
    cur.execute('CREATE TABLE IF NOT EXISTS teams(id INTEGER PRIMARY KEY, name VARCHAR)')
    cur.execute('CREATE TABLE IF NOT EXISTS chips(id INTEGER PRIMARY KEY, chips VARCHAR, CONSTRAINT FK_man_chips FOREIGN KEY (id) REFERENCES managers(id))')
    cur.execute('CREATE TABLE IF NOT EXISTS positions(id INTEGER PRIMARY KEY, name VARCHAR)')
    cur.execute('CREATE TABLE IF NOT EXISTS fixtures(gw INTEGER, id INTEGER, home_team INTEGER, away_team INTEGER, home_diff INTEGER, away_diff INTEGER, CONSTRAINT PK_fix PRIMARY KEY (gw, home_team, away_team), CONSTRAINT FK_team_fix1 FOREIGN KEY (home_team) REFERENCES teams(id), CONSTRAINT FK_team_fix2 FOREIGN KEY (away_team) REFERENCES teams(id))')
    cur.execute('CREATE TABLE IF NOT EXISTS players(id INTEGER PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, web_name VARCHAR, team_id INTEGER, pos_id INTEGER)')
    cur.execute('CREATE TABLE IF NOT EXISTS picks(gw INTEGER, ent_id INTEGER, el_id INTEGER, mult INTEGER, points INTEGER, CONSTRAINT PK_picks PRIMARY KEY (gw, ent_id, el_id), CONSTRAINT FK_man_pick FOREIGN KEY (ent_id) REFERENCES managers(id), CONSTRAINT FK_play_pick FOREIGN KEY (el_id) REFERENCES players(id))')
    cur.execute('CREATE TABLE IF NOT EXISTS stat(gw INTEGER, id INTEGER, every INTEGER, transfers INTEGER, capt INTEGER, vcapt INTEGER, bench INTEGER, value FLOAT, place INTEGER, CONSTRAINT PK_stat PRIMARY KEY(gw, id), CONSTRAINT FK_man_stat FOREIGN KEY (id) REFERENCES managers(id))')
    cur.execute('CREATE TABLE IF NOT EXISTS transfers(gw INTEGER, id INTEGER, transfers_in VARCHAR, transfers_out VARCHAR, CONSTRAINT PK_trans PRIMARY KEY(gw, id), CONSTRAINT FK_man_trans FOREIGN KEY (id) REFERENCES managers(id))')
    cur.execute('CREATE TABLE IF NOT EXISTS attributes(gw INTEGER, id INTEGER, event_points INTEGER, ict FLOAT, form FLOAT, value FLOAT, xPTS FLOAT, against VARCHAR, CONSTRAINT PK_att PRIMARY KEY (gw, id), CONSTRAINT FK_play_attr FOREIGN KEY (id) REFERENCES players(id))')
        
    con.commit()

def init_managers():
    r = requests.get('https://fantasy.premierleague.com/api/leagues-classic/2323/standings/')
    
    json = r.json()
    print(json.keys())
    temp = pd.DataFrame(json['new_entries'])['results']
    
    for i in range(len(temp)):
        temp[i]['player_name'] = str(temp[i]['player_first_name'] + ' ' + temp[i]['player_last_name'])
        sql = "INSERT INTO managers (id, club, name) VALUES (?, ?, ?)"
        val = [(str(temp[i]['entry']), str(temp[i]['entry_name']), str(temp[i]['player_name']))]
        try:
            cur.executemany(sql, val)
        except Exception as err:
            print('Query failed: %s; continuing' % (str(err)))

def init_teams():
    r = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/')
    json = r.json()
    temp = json['teams']
    for i in range(len(temp)):
        sql = "INSERT INTO teams (id, name) VALUES (?, ?)"
        val = [(temp[i]['id'], str(temp[i]['name']))]
        try:
            cur.executemany(sql, val)
        except Exception as err:
            print('Query failed: %s; continuing' % (str(err)))
            
def init_fixtures():
    r = requests.get('https://fantasy.premierleague.com/api/fixtures/')
    json = r.json()
    temp_fix = pd.DataFrame(json)
    fix = temp_fix[['event','team_h','team_a','team_h_difficulty','team_a_difficulty']]
    try:
        fix.to_sql('fixtures', con=con, if_exists='replace', index=False)
    except Exception as err:
        print('Query failed: %s; continuing' % (str(err)))
        
def init_players():
    r = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/')
    json = r.json()
    temp = pd.DataFrame(json['elements'])
    temp = temp[['id','first_name','second_name','team','element_type']]
    try:
        temp.to_sql('players', con=con, if_exists='replace', index=False)
    except Exception as err:
        print('Query failed: %s; continuing' % (str(err)))

def refresh_attributes(GW):
    r = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/')
    json = r.json()
    temp = pd.DataFrame(json['elements'])
    
    attr = pd.DataFrame()
    attr = temp[['id','ep_this','ict_index','form','now_cost','ep_next']]
    
    players = pd.read_sql("SELECT * FROM players", con)
    play_attr = pd.merge(players, attr, left_on = 'id', right_on = 'id')
    
    fixtures = pd.read_sql('SELECT * FROM fixtures WHERE event = '+ str(GW), con)
    
    play_attr_fix_h = pd.merge(play_attr, fixtures, left_on = 'team', right_on = 'team_h')
    play_attr_fix_h = play_attr_fix_h[['event','id','ep_this','ict_index','form','now_cost','ep_next','team_a']]
    play_attr_fix_h = play_attr_fix_h.rename(columns = {'team_a': 'against'})
    play_attr_fix_a = pd.merge(play_attr, fixtures, left_on = 'team', right_on = 'team_a')
    play_attr_fix_a = play_attr_fix_a[['event','id','ep_this','ict_index','form','now_cost','ep_next','team_h']]
    play_attr_fix_a = play_attr_fix_a.rename(columns = {'team_h': 'against'})
    
    attr_ag = play_attr_fix_h.append(play_attr_fix_a).sort_values(by = 'id').rename(columns = {'event': 'gw', 'ep_this': 'event_points', 'ict_index':'ict','ep_next':'xPTS','now_cost':'value'})
    attr_ag = attr_ag.fillna(value=0)
    
    try:
        #cur.execute("DELETE from attributes WHERE GW = ",GW)
        attr_ag.to_sql('attributes', con=con, if_exists='append', index=False)
    except Exception as err:
        print('Query failed: %s; continuing' % (str(err)))