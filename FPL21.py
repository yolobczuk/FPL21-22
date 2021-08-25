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
    cur.execute('CREATE TABLE IF NOT EXISTS players(id INTEGER PRIMARY KEY, web_name VARCHAR, web_name VARCHAR, team_id INTEGER, pos_id INTEGER)')
    cur.execute('CREATE TABLE IF NOT EXISTS picks(gw INTEGER, ent_id INTEGER, el_id INTEGER, mult INTEGER, points INTEGER, CONSTRAINT PK_picks PRIMARY KEY (gw, ent_id, el_id), CONSTRAINT FK_man_pick FOREIGN KEY (ent_id) REFERENCES managers(id), CONSTRAINT FK_play_pick FOREIGN KEY (el_id) REFERENCES players(id))')
    cur.execute('CREATE TABLE IF NOT EXISTS stat(name VARCHAR, club VARCHAR, gw INTEGER, total INTEGER, transfers INTEGER, capt INTEGER, bench INTEGER, value FLOAT, CONSTRAINT PK_stat PRIMARY KEY(gw, name, club))')
    cur.execute('CREATE TABLE IF NOT EXISTS transfers(gw INTEGER, id INTEGER, transfers_in VARCHAR, transfers_out VARCHAR, CONSTRAINT PK_trans PRIMARY KEY(gw, id), CONSTRAINT FK_man_trans FOREIGN KEY (id) REFERENCES managers(id))')
    cur.execute('CREATE TABLE IF NOT EXISTS attributes(gw INTEGER, id INTEGER, event_points INTEGER, ict FLOAT, form FLOAT, value FLOAT, xPTS FLOAT, against VARCHAR, CONSTRAINT PK_att PRIMARY KEY (gw, id), CONSTRAINT FK_play_attr FOREIGN KEY (id) REFERENCES players(id))')
    cur.execute('CREATE TABLE IF NOT EXISTS transfer_history(gw INTEGER, id INTEGER, cost INTEGER, CONSTRAINT PK_trhis PRIMARY KEY (gw, id), CONSTRAINT FK_play_trhist FOREIGN KEY (id) REFERENCES managers(id))')
    con.commit()

def init_managers():
    r = requests.get('https://fantasy.premierleague.com/api/leagues-classic/2323/standings/')
    
    json = r.json()
    temp = pd.DataFrame(json['new_entries'])['results']
    
    for i in range(len(temp)):
        temp[i]['player_name'] = str(temp[i]['player_first_name'] + ' ' + temp[i]['player_last_name'])
        sql = "INSERT INTO managers (id, club, name) VALUES (?, ?, ?)"
        val = [(str(temp[i]['entry']), str(temp[i]['entry_name']), str(temp[i]['player_name']))]
        try:
            cur.executemany(sql, val)
        except Exception as err:
            print('Query failed: %s; continuing' % (str(err)))
    con.commit()
            
def show_databases():
    print("MANAGERS IN THE LEAGUE:")
    cur.execute("SELECT * FROM managers")
    rows = cur.fetchall()
    for row in rows:
        print(row[0], row[2])
        
    print("TEAMS:")
    cur.execute("SELECT * FROM teams")
    rows = cur.fetchall()
    for row in rows:
        print(row[0], row[1])
    
    print("FIXTURES:")
    cur.execute("SELECT * FROM fixtures")
    rows = cur.fetchall()
    for row in rows:
        print(row[0], row[1])
        
    print("POSITIONS:")
    cur.execute("SELECT * FROM positions")
    rows = cur.fetchall()
    for row in rows:
        print(row[0], row[1])
        
    print("PLAYERS:")
    cur.execute("SELECT * FROM players")
    rows = cur.fetchall()
    for row in rows:
        print(row[0], row[1])

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
    con.commit()
            
def init_fixtures():
    r = requests.get('https://fantasy.premierleague.com/api/fixtures/')
    json = r.json()
    temp_fix = pd.DataFrame(json)
    fix = temp_fix[['event','team_h','team_a','team_h_difficulty','team_a_difficulty']]
    try:
        fix.to_sql('fixtures', con=con, if_exists='replace', index=False)
    except Exception as err:
        print('Query failed: %s; continuing' % (str(err)))
    con.commit()
        
def init_players():
    r = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/')
    json = r.json()
    temp = pd.DataFrame(json['elements'])
    temp = temp[['id','web_name','team','element_type']]
    try:
        temp.to_sql('players', con=con, if_exists='replace', index=False)
    except Exception as err:
        print('Query failed: %s; continuing' % (str(err)))
    con.commit()
        
def init_positions():
    r = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/')
    json = r.json()
    temp = pd.DataFrame(json['element_types'])[['id','singular_name']]
    for i in range(len(temp)):
        sql = "INSERT INTO positions (id, name) VALUES (?, ?)"
        val = [(int(temp['id'][i]), str(temp['singular_name'][i]))]
        try:
            cur.executemany(sql, val)
        except Exception as err:
            print('Query failed: %s; continuing' % (str(err)))
    con.commit()
            
def fill_databases():
    init_teams()
    init_fixtures()
    init_players()
    init_positions()

def refresh_attributes(GW):
    r = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/')
    json = r.json()
    temp = pd.DataFrame(json['elements'])
    
    attr = pd.DataFrame()
    attr = temp[['id','event_points','ict_index','form','now_cost','ep_next']]
    
    players = pd.read_sql("SELECT * FROM players", con)
    play_attr = pd.merge(players, attr, left_on = 'id', right_on = 'id')
    
    fixtures = pd.read_sql('SELECT * FROM fixtures WHERE event = '+ str(GW), con)
    
    play_attr_fix_h = pd.merge(play_attr, fixtures, left_on = 'team', right_on = 'team_h')
    play_attr_fix_h = play_attr_fix_h[['event','id','event_points','ict_index','form','now_cost','ep_next','team_a']]
    play_attr_fix_h = play_attr_fix_h.rename(columns = {'team_a': 'against'})
    play_attr_fix_a = pd.merge(play_attr, fixtures, left_on = 'team', right_on = 'team_a')
    play_attr_fix_a = play_attr_fix_a[['event','id','event_points','ict_index','form','now_cost','ep_next','team_h']]
    play_attr_fix_a = play_attr_fix_a.rename(columns = {'team_h': 'against'})
    
    attr_ag = play_attr_fix_h.append(play_attr_fix_a).sort_values(by = 'id').rename(columns = {'event': 'gw', 'ict_index':'ict','ep_next':'xPTS','now_cost':'value'})
    attr_ag = attr_ag.fillna(value=0)
    
    attr_ag['value'] = attr_ag['value']/10
    
    
    try:
        cur.execute("DELETE FROM attributes WHERE gw = " + str(GW))
        attr_ag.to_sql('attributes', con=con, if_exists='append', index=False)
    except Exception as err:
        print('Query failed: %s; continuing' % (str(err)))
        
def init_picks(GW):
    refresh_attributes(GW)
    attributes = pd.read_sql("SELECT * FROM attributes WHERE gw = " + str(GW), con = con)
    managers = pd.read_sql("SELECT * FROM managers", con = con)
    transfers = pd.DataFrame()
    try:
        cur.execute("DELETE FROM picks WHERE gw = " + str(GW))
    except Exception as err:
        print('Query 1 failed: %s; continuing' % (str(err)))
    con.commit()
    for i in managers['id']:
        print('Now processing',i)
        url = 'https://fantasy.premierleague.com/api/entry/' + str(i) + '/event/' + str(GW) + '/picks/'
        r = requests.get(url)
        json = r.json()
        picks = pd.DataFrame(json['picks'])[['element', 'multiplier']]
        picks = pd.merge(picks, attributes, left_on = 'element', right_on = 'id')
        picks['gw'] = GW
        picks['ent_id'] = i
        picks = picks[['gw','ent_id','element','multiplier','event_points']]
        picks.columns = ['gw','ent_id','el_id','mult','points']
        transfers = transfers.append([[GW, i, json['entry_history']['event_transfers_cost']]])
        try:
            picks.to_sql('picks', con=con, if_exists='append', index=False)
        except Exception as err:
            print('Query failed II: %s; continuing' % (str(err)))
    transfers.columns = ['gw', 'id', 'cost']
    try:
        cur.execute("DELETE FROM transfer_history WHERE gw = " + str(GW))
        con.commit()
        transfers.to_sql('transfer_history', con=con, if_exists='append', index=False)
    except Exception as err:
        print('Query failed: %s; continuing' % (str(err)))
            
def init_player_info(GW):
    choice = input("Do you want to initialize picks? Y/N ")
    if choice == 'Y':
        init_picks(GW)
    try:
        cur.execute("DROP VIEW player_info")
    except Exception as err:
        print('Query failed: %s; continuing' % (str(err)))
    cur.execute("CREATE VIEW player_info AS SELECT ent_id, p.gw, name, club, mult, points, value, a.against, pl.web_name, team, element_type FROM picks p INNER JOIN managers m ON m.id = p.ent_id INNER JOIN attributes a ON a.id = p.el_id AND a.gw = p.gw INNER JOIN players pl ON a.id = pl.id")
    
def write_summary(GW):
    init_player_info(GW)
    info = pd.read_sql("SELECT * FROM player_info WHERE gw = " + str(GW), con = con)
    managers = pd.read_sql("SELECT * FROM managers", con = con)
    captain = info[info['mult']>=2][['name','web_name']]
    summary = pd.DataFrame()
    for i in range(len(managers['id'])):
        url = 'https://fantasy.premierleague.com/api/entry/' + str(managers['id'][i]) + '/event/' + str(GW) + '/picks/'
        r = requests.get(url)
        json = r.json()
        chips = json['active_chip']
        transfers = -json['entry_history']['event_transfers_cost']
        add = [[managers['name'][i], chips, transfers]]
        summary = summary.append(add)
    summary.columns = ['name','active_chip','transfer_cost']
    captain = pd.merge(captain, summary, left_on = 'name', right_on = 'name')
    captain = captain.rename(columns = {'web_name':'captain'})
    print(captain)
    
    
def init_stats(ver = 'season', GW = None):
    if(ver == 'season'):
        info = pd.read_sql("SELECT * FROM player_info", con = con)
    else:
        info = pd.read_sql("SELECT * FROM player_info WHERE gw = " + str(GW), con = con)
    positions = pd.read_sql("SELECT * FROM positions", con = con).set_index('id')
    teams = pd.read_sql("SELECT * FROM teams", con = con).set_index('id')
    
    info['against'] = info.against.map(teams.name)
    info['team'] = info.team.map(teams.name)
    info['element_type'] = info.element_type.map(positions.name)
    info['mult_points'] = info['mult']*info['points']
    
    fs = pd.pivot_table(info[info['mult']>0], values='mult_points', index=['name','club'], aggfunc=np.sum).sort_values(by='mult_points', ascending=False)
    b = pd.pivot_table(info[info['mult']==0], values='points', index=['name','club'], aggfunc=np.sum).sort_values(by='points', ascending=False)
    c = pd.pivot_table(info[info['mult']>=2], values='mult_points', index=['name','club'], aggfunc=np.sum).sort_values(by='mult_points', ascending=False)
    v = pd.pivot_table(info[info['gw']==GW], values='value', index=['name','club'], aggfunc=np.sum).sort_values(by='value', ascending=False)
    t = pd.read_sql("SELECT t.gw, m.name, m.club, t.cost FROM managers m INNER JOIN transfer_history t ON t.id = m.id",con=con).set_index(['name','club'])
    t = t.cost.groupby(['name','club']).sum()
    
    summ = pd.concat([fs, b, c, v, t], axis=1)
    summ.columns = ['total','bench','captain','value','transfers']
    summ['total'] = summ['total'] - summ['transfers']
    summ = summ.sort_values(by = 'total',ascending = False)
    summ['next'] = abs(summ['total'].diff()).fillna(0)
    summ['leader'] = abs(summ['total'] - summ['total'].iloc[0])
    if(ver == 'gw'):
        summ['gw'] = GW
        summ = summ[['gw','total','next','leader','transfers','captain','bench','value']]
        choice = input("Do you want to save to CSV? Y/N ")
        if choice == 'Y':
            summ.to_csv('stat_gw.csv', index=True, mode = 'a', encoding='utf-8-sig')
        try:
            cur.execute("DELETE FROM stat WHERE gw = " + str(GW))
            summ.to_sql('stat', con=con, if_exists='append', index=False)
        except Exception as err:
            print('Query failed: %s; continuing' % (str(err)))
        print("GW " + str(GW) + " STATS")
        print(summ)
    else:
        summ = summ[['total','next','leader','transfers','captain','bench','value']]
        choice = input("Do you want to save to CSV? Y/N ")
        if choice == 'Y':
            summ.to_csv('stat_season.csv', index=True, encoding='utf-8-sig')
        print("SEASON STATS")
        print(summ)
        
def export_picks():
    info = pd.read_sql("SELECT * FROM player_info WHERE mult > 0", con = con)
    positions = pd.read_sql("SELECT * FROM positions", con = con).set_index('id')
    teams = pd.read_sql("SELECT * FROM teams", con = con).set_index('id')
    
    info['against'] = info.against.map(teams.name)
    info['team'] = info.team.map(teams.name)
    info['element_type'] = info.element_type.map(positions.name)
    info['mult_points'] = info['mult']*info['points']
    info[info['mult']>0].to_csv('data.csv', index=False, encoding='utf-8-sig')
    info[info['mult']>1].to_csv('captains.csv', index=False, encoding='utf-8-sig')
    
def print_means():
    info = pd.read_sql("SELECT * FROM player_info WHERE mult > 0", con = con)
    positions = pd.read_sql("SELECT * FROM positions", con = con).set_index('id')
    teams = pd.read_sql("SELECT * FROM teams", con = con).set_index('id')
    
    info['against'] = info.against.map(teams.name)
    info['team'] = info.team.map(teams.name)
    info['element_type'] = info.element_type.map(positions.name)
    info['mult_points'] = info['mult']*info['points']
    print("POINT MEANS, BY NAME")
    print(info[['web_name','mult_points']].groupby(['web_name']).mean().sort_values(by = 'mult_points', ascending = False).head(10))
    
    print("POINT MEANS, BY POSITION")
    print(info[['element_type','mult_points']].groupby(['element_type']).mean().sort_values(by = 'mult_points', ascending = False).head(10))
    
    print("POINT MEANS, BY TEAM")
    print(info[['team','mult_points']].groupby(['team']).mean().sort_values(by = 'mult_points', ascending = False).head(10))
    
    print("POINT MEANS, BY AGAINST")
    print(info[['against','mult_points']].groupby(['against']).mean().sort_values(by = 'mult_points', ascending = False).head(10))
    
def print_counts(ver = 'season'):
    if(ver == 'season'):
        info = pd.read_sql("SELECT * FROM player_info WHERE mult > 0", con = con)
        print("SEASON COUNTS")
    else:
        GW = int(input("Pick GW: "))
        info = pd.read_sql("SELECT * FROM player_info WHERE mult > 0 AND gw = " + str(GW), con = con)
        print("GW " + str(GW) +" COUNTS")
    positions = pd.read_sql("SELECT * FROM positions", con = con).set_index('id')
    teams = pd.read_sql("SELECT * FROM teams", con = con).set_index('id')
    
    info['against'] = info.against.map(teams.name)
    info['team'] = info.team.map(teams.name)
    info['element_type'] = info.element_type.map(positions.name)
    info['mult_points'] = info['mult']*info['points']
    
    print("COUNTS, BY NAME")
    print(info[['web_name','mult_points']].groupby(['web_name']).count().sort_values(by = 'mult_points', ascending = False).head(10))
    
    print("COUNTS, BY POSITION")
    print(info[['element_type','mult_points']].groupby(['element_type']).count().sort_values(by = 'mult_points', ascending = False).head(10))
    
    print("COUNTS, BY TEAM")
    print(info[['team','mult_points']].groupby(['team']).count().sort_values(by = 'mult_points', ascending = False).head(10))
    
    print("COUNTS, BY AGAINST")
    print(info[['against','mult_points']].groupby(['against']).count().sort_values(by = 'mult_points', ascending = False).head(10))
    
    
OK = True

while OK:
    print("MAIN MENU")
    print("1. Initialise databases")
    print("2. Update managers")
    print("3. Fill databases")
    print("4. Show databases")
    print("5. Refresh attributes")
    print("6. Initialize picks")
    print("7. Print summary")
    print("8. Print means")    
    print("9. Print counts")  
    print("10. Initialize gw stats")
    print("11. Initialize season stats")
    print("12. Export data")
    
    print("0. Exit")
    wybor = input("Pick a number: ")

    if wybor == '1':
        init_db()
        
    elif wybor == '2':
        init_managers()
    
    elif wybor == '3':
        fill_databases()
            
    elif wybor == '4':
        show_databases()
        
    elif wybor == '5':
        GW = input("Pick GW: ")
        refresh_attributes(GW)

    elif wybor == '6':
        GW = input("Pick GW: ")
        init_picks(GW)

    elif wybor == '7':
        GW = int(input("Pick GW: "))
        write_summary(GW)

    elif wybor == '8':
        print_means()

    elif wybor == '9':
        ver = input("Choose version (season/gw): ")
        print_counts(ver)

    elif wybor == '10':
        GW = int(input("Pick GW: "))
        init_stats('gw',GW)

    elif wybor == '11':
        GW = int(input("What is current GW? "))
        init_stats('season', GW)   

    elif wybor == '12':
        export_picks()
        
    elif wybor == '13':
        GW = int(input("Pick GW: "))

    elif wybor == 'D':
        print(pd.read_sql("SELECT * FROM attributes",con=con))
        
    elif wybor == 'E':
        print(pd.read_sql("SELECT * FROM picks",con=con))

    elif wybor == '0':
        print("EXIT")
        OK = False
        
    else:
        print("Wrong number")
     