import json
import pyodbc
import os
import logging
import requests
from bs4 import BeautifulSoup
import time
import unicodedata
import pandas as pd
from datetime import datetime
from time import sleep
import boto3
from utils.db_utils import get_db_connection



# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def find_new_games(cursor, when='last_three_days'):
    logger.info(f"Finding new games for period: {when}")
    # Your existing find_new_games code here
    if when == 'yesterday':      
        seasonQuery = """
        SELECT gameId
        FROM [dbo].[LeagueSchedule24_25]
        WHERE CAST(gameDateTimeEst AS DATE) < CAST(GETDATE() AS DATE)
        """
    elif when == 'today':
        seasonQuery = """
        SELECT gameId
        FROM [dbo].[LeagueSchedule24_25]
        WHERE CAST(gameDateTimeEst AS DATE) <= CAST(GETDATE() AS DATE)
        """
    elif when == 'tomorrow':
        seasonQuery = """
        SELECT gameId
        FROM [dbo].[LeagueSchedule24_25]
        WHERE CAST(gameDateTimeEst AS DATE) <= CAST(DATEADD(DAY, 1, GETDATE()) AS DATE)
        """ 
    elif when == 'last_three_days':
        seasonQuery = """
        SELECT gameId
        FROM [dbo].[LeagueSchedule24_25]
        WHERE CAST(gameDateTimeEst AS DATE) BETWEEN 
            CAST(DATEADD(DAY, -3, GETDATE()) AS DATE) AND 
            CAST(DATEADD(DAY, -1, GETDATE()) AS DATE)
        """

    unfound_games = [row[0] for row in cursor.execute(seasonQuery)]

    return set(unfound_games)


def get_new_games(unfound_games):
    logger.info(f"Retrieving {len(unfound_games)} games from NBA.com")
    found_games = set()
    attempts = 0
    games_list = []
    while len(unfound_games) > 0 and attempts < 3:
        for game_id in list(unfound_games):
            logger.info(f'Trying game {game_id}...')
            url = f'https://www.nba.com/game/00{str(game_id)}'
            try:

                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Referer': 'https://www.nba.com',
                    'Upgrade-Insecure-Requests': '1',
                    'Cache-Control': 'max-age=0' 
                }
               
                
                response = requests.get(url, headers=headers, timeout=10)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    script_tag = soup.find('script', type='application/json')
                    if script_tag:
                        json_data = json.loads(script_tag.string)
                        game = json_data.get('props', {}).get('pageProps', {}).get('game')
                        if game:
                            found_games.add(game_id)
                            games_list.append(game)
                            logger.info(f'Successfully retrieved game {game_id}')
                        else:
                            logger.warning(f'Game {game_id} not found in JSON')
                    else:
                        logger.warning(f'Script tag not found for game {game_id}')
                else:
                    logger.error(f'Status code error {response.status_code} for game {game_id}')
            except Exception as e:
                logger.error(f'Error processing game {game_id}: {str(e)}')
            
            time.sleep(2)  # Rate limiting
        unfound_games = unfound_games - found_games
        time.sleep(10)
        attempts += 1
    return games_list

def collect_all_players(games_list,conn):
    
    def sanitize(value):
        """
        Replace null-like values (NaN, None, empty strings) with None.
        """
        if value is None or (isinstance(value, str) and value.strip() == '') or (isinstance(value, float) and pd.isna(value)):
            return None
        return value
    
    def remove_accents(input_str):
        input_str = sanitize(input_str)
        if isinstance(input_str, str):
            nfkd_form = unicodedata.normalize('NFD', input_str)
            return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])
        else:
            return None
    
    def checkUndrafted(draftValue):
        draftValue = sanitize(draftValue)
        if isinstance(draftValue,str) and draftValue.lower() == 'undrafted':
            return -1
        else:
            return draftValue
        
    def getHeight(height):
        h = sanitize(person['height'])
        if isinstance(h, str):
            height = int(h.split('-')[0]) * 12 + int(h.split('-')[1]) if h and '-' in h else None
        else:
            height = None
        return height 
    
    all_players = [] 
    id_set = set()
    db_ids = []
    non_db_ids=[]
    
    player_df = pd.read_sql("select * from CommonPlayerInfo", conn)
    
    for game in games_list:
        # Process both home and away teams

        for player in game['homeTeam']['players'] + game['awayTeam']['players']:
            personId = player['personId']
            if personId not in id_set:
                try:
                    matches = player_df.loc[player_df['person_id'] == personId, :]
                    if len(matches) == 0:
                        raise KeyError("ID not found in DataFrame")
                    person = player_df.loc[player_df['person_id'] == personId, :].squeeze()
                    # Extract and sanitize fields
                    firstName = remove_accents(person['first_name'])
                    lastName = remove_accents(person['last_name'])
                    birthDate = sanitize(person['birthdate'])
                    school = sanitize(person['school'])
                    country = sanitize(person['country'])
                    # Handle height
                    height = getHeight(person['height'])
                    bodyWeight = sanitize(person['weight'])
                    # Position check
                    position = sanitize(person.get('position', ''))
                    guard = 'guard' in position.lower() if position else None
                    forward = 'forward' in position.lower() if position else None
                    center = 'center' in position.lower() if position else None
                    # Draft year, round, and number with undrafted check
                    draftYear = checkUndrafted(person['draft_year'])
                    draftRound = checkUndrafted(person['draft_round'])
                    draftNumber = checkUndrafted(person['draft_number'])
                    dleague = sanitize(person['dleague_flag']) == 'Y'
                    db_ids.append(personId)
                except Exception as e:
                    firstName = remove_accents(sanitize(player['firstName']))
                    lastName = remove_accents(sanitize(player['familyName']))
                    birthDate = None
                    school = None
                    country = None
                    height = None
                    bodyWeight = None
                    position = sanitize(player.get('position',''))
                    guard = 'guard' in position.lower() if position else None
                    forward = 'forward' in position.lower() if position else None
                    center = 'center' in position.lower() if position else None              
                    draftYear = None 
                    draftRound = None
                    draftNumber = None
                    dleague = None                   
                    non_db_ids.append(personId)    
                player_tuple = ([
                    personId,
                    firstName,
                    lastName,
                    birthDate,
                    school,
                    country,
                    height,
                    bodyWeight,
                    guard,
                    forward,
                    center,
                    draftYear,
                    draftRound,
                    draftNumber,
                    dleague   
                ])
                all_players.append(player_tuple)
            id_set.add(personId)
    return all_players, db_ids, non_db_ids


def insert_players(cursor, all_players):
    if not all_players:
        return  # No players to insert
        
    # Create a temporary table with the same structure
    cursor.execute("""
    CREATE TABLE #TempPlayers (
        personId INT PRIMARY KEY,
        firstName NVARCHAR(50),
        lastName NVARCHAR(50),
        birthDate DATE,
        school NVARCHAR(100),
        country NVARCHAR(50),
        height INT,
        bodyWeight INT,
        guard BIT,
        forward BIT,
        center BIT,
        draftYear INT,
        draftRound INT,
        draftNumber INT,
        dleague BIT
    )
    """)
    
    # Insert into temp table
    insert_temp_sql = """
    INSERT INTO #TempPlayers VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?
    )
    """
    cursor.executemany(insert_temp_sql, all_players)
    
    # Merge from temp table to main table
    cursor.execute("""
    MERGE INTO Players AS target
    USING #TempPlayers AS source
    ON target.personId = source.personId
    WHEN MATCHED THEN
        UPDATE SET
            firstName = source.firstName,
            lastName = source.lastName,
            birthDate = source.birthDate,
            school = source.school,
            country = source.country,
            height = source.height,
            bodyWeight = source.bodyWeight,
            guard = source.guard,
            forward = source.forward,
            center = source.center,
            draftYear = source.draftYear,
            draftRound = source.draftRound,
            draftNumber = source.draftNumber,
            dleague = source.dleague
    WHEN NOT MATCHED THEN
        INSERT VALUES (
            source.personId,
            source.firstName,
            source.lastName,
            source.birthDate,
            source.school,
            source.country,
            source.height,
            source.bodyWeight,
            source.guard,
            source.forward,
            source.center,
            source.draftYear,
            source.draftRound,
            source.draftNumber,
            source.dleague
        );
    
    DROP TABLE #TempPlayers;
    """)



def parse_duration(duration_str):
    if not duration_str:
        return None
    try:
        # Handle 'minutes:seconds' format
        if ':' in duration_str:
            minutes, _ = duration_str.split(':')
            return float(minutes)
        # Handle ISO format 'PT00M00.00S'
        elif duration_str.startswith('PT'):
            # Extract minutes between 'T' and 'M'
            minutes_str = duration_str[duration_str.find('T')+1:duration_str.find('M')]
            if minutes_str:
                return float(minutes_str)
        return None
    except (ValueError, AttributeError):
        return None


def collect_games(games_list):
    games_data = []
    for game in games_list:
        attendance = game['attendance'] 
        if attendance ==0:
            attendance = None
        minutes_str = game[f'homeTeam']['statistics'].get('minutes', None)
        if not minutes_str:
            # Try getting the duration directly from the game object
            minutes_str = game.get('duration')
            
        gameDuration = parse_duration(minutes_str)
        if gameDuration is not None and gameDuration >=120:
            gameDuration = gameDuration/5
        
        
        games_data.append((
            int(game['gameId']),
            game['gameEt'], #gameDate
            gameDuration,
            game['homeTeam']['teamId'],
            game['awayTeam']['teamId'],
            game['homeTeam']['score'],
            game['awayTeam']['score'],
            game['homeTeam']['teamId'] if game['homeTeam']['score'] > game['awayTeam']['score'] else game['awayTeam']['teamId'],
            attendance
        ))
    return games_data
  
  

def insert_games(cursor, games_data):
    if not games_data:
        print("No games to insert")
        return
        
    # First debug the data we're trying to insert
    if games_data:
        print("\nSample game data structure:")
        print(f"Number of fields: {len(games_data[0])}")
        print("Values:", games_data[0])
        
    # Create temporary table
    cursor.execute("""
    CREATE TABLE #TempGames (
        gameId NVARCHAR(20) PRIMARY KEY,
        gameDate DATETIME,
        gameDuration DECIMAL(6,3),
        hometeamId INT,
        awayteamId INT,
        homeScore INT,
        awayScore INT,
        winner INT,
        attendance INT
    )
    """)
    
    # Insert into temp table
    insert_temp_sql = """
    INSERT INTO #TempGames (
        gameId,
        gameDate,
        gameDuration,
        hometeamId,
        awayteamId,
        homeScore,
        awayScore,
        winner,
        attendance
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    try:
        cursor.executemany(insert_temp_sql, games_data)
    except Exception as e:
        print(f"\nError inserting into temp table: {str(e)}")
        cursor.execute("DROP TABLE #TempGames")
        raise
    
    # Merge from temp table to main table
    try:
        cursor.execute("""
        MERGE INTO Games AS target
        USING #TempGames AS source
        ON target.gameId = source.gameId
        WHEN MATCHED THEN
            UPDATE SET
                gameDate = source.gameDate,
                gameDuration = source.gameDuration,
                hometeamId = source.hometeamId,
                awayteamId = source.awayteamId,
                homeScore = source.homeScore,
                awayScore = source.awayScore,
                winner = source.winner,
                attendance = source.attendance
        WHEN NOT MATCHED THEN
            INSERT (
                gameId,
                gameDate,
                gameDuration,
                hometeamId,
                awayteamId,
                homeScore,
                awayScore,
                winner,
                attendance
            ) VALUES (
                source.gameId,
                source.gameDate,
                source.gameDuration,
                source.hometeamId,
                source.awayteamId,
                source.homeScore,
                source.awayScore,
                source.winner,
                source.attendance
            );
        """)
    except Exception as e:
        print(f"\nError during merge operation: {str(e)}")
        raise
    finally:
        cursor.execute("DROP TABLE #TempGames")

def insert_player_stats(cursor, players_stats):
    if not players_stats:
        return
        
    # Create temporary table
    cursor.execute("""
    CREATE TABLE #TempPlayerStats (
        personId INT,
        gameId INT,
        teamId INT,
        assists INT,
        blocks INT,
        fieldGoalsAttempted INT,
        fieldGoalsMade INT,
        fieldGoalsPercentage FLOAT,
        foulsPersonal INT,
        freeThrowsAttempted INT,
        freeThrowsMade INT,
        freeThrowsPercentage FLOAT,
        numMinutes INT,
        plusMinusPoints INT,
        points INT,
        reboundsDefensive INT,
        reboundsOffensive INT,
        reboundsTotal INT,
        steals INT,
        threePointersAttempted INT,
        threePointersMade INT,
        threePointersPercentage FLOAT,
        turnovers INT,
        PRIMARY KEY (personId, gameId)
    )
    """)
    
    # Insert into temp table in batches
    batch_size = 3000
    insert_temp_sql = """
    INSERT INTO #TempPlayerStats VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?
    )
    """
    
    for i in range(0, len(players_stats), batch_size):
        batch = players_stats[i:i + batch_size]
        cursor.executemany(insert_temp_sql, batch)
        print(f"Processed batch {i} to {min(i + batch_size, len(players_stats))}")
    
    # Merge from temp table to main table
    cursor.execute("""
    MERGE INTO PlayerStatistics AS target
    USING #TempPlayerStats AS source
    ON target.personId = source.personId AND target.gameId = source.gameId
    WHEN MATCHED THEN
        UPDATE SET
            teamId = source.teamId,
            assists = source.assists,
            blocks = source.blocks,
            fieldGoalsAttempted = source.fieldGoalsAttempted,
            fieldGoalsMade = source.fieldGoalsMade,
            fieldGoalsPercentage = source.fieldGoalsPercentage,
            foulsPersonal = source.foulsPersonal,
            freeThrowsAttempted = source.freeThrowsAttempted,
            freeThrowsMade = source.freeThrowsMade,
            freeThrowsPercentage = source.freeThrowsPercentage,
            numMinutes = source.numMinutes,
            plusMinusPoints = source.plusMinusPoints,
            points = source.points,
            reboundsDefensive = source.reboundsDefensive,
            reboundsOffensive = source.reboundsOffensive,
            reboundsTotal = source.reboundsTotal,
            steals = source.steals,
            threePointersAttempted = source.threePointersAttempted,
            threePointersMade = source.threePointersMade,
            threePointersPercentage = source.threePointersPercentage,
            turnovers = source.turnovers
    WHEN NOT MATCHED THEN
        INSERT VALUES (
            source.personId,
            source.gameId,
            source.teamId,
            source.assists,
            source.blocks,
            source.fieldGoalsAttempted,
            source.fieldGoalsMade,
            source.fieldGoalsPercentage,
            source.foulsPersonal,
            source.freeThrowsAttempted,
            source.freeThrowsMade,
            source.freeThrowsPercentage,
            source.numMinutes,
            source.plusMinusPoints,
            source.points,
            source.reboundsDefensive,
            source.reboundsOffensive,
            source.reboundsTotal,
            source.steals,
            source.threePointersAttempted,
            source.threePointersMade,
            source.threePointersPercentage,
            source.turnovers
        );
        
    DROP TABLE #TempPlayerStats;
    """)

def insert_teams(cursor, team_ids):
    """
    Insert or update teams in the Teams table.
    Uses a temporary table and MERGE statement to handle both new and existing teams.
    
    Args:
        cursor: Database cursor
        team_ids: List of team IDs to insert/update
    """
    if not team_ids:
        print("No teams to insert")
        return
        
    # Create temporary table
    cursor.execute("""
    CREATE TABLE #TempTeams (
        teamId INT PRIMARY KEY
    )
    """)
    
    # Convert to tuples for executemany
    team_tuples = [(team_id,) for team_id in team_ids]
    
    # Insert into temp table
    insert_temp_sql = "INSERT INTO #TempTeams (teamId) VALUES (?)"
    cursor.executemany(insert_temp_sql, team_tuples)
    
    # Merge from temp table to main table
    cursor.execute("""
    MERGE INTO Teams AS target
    USING #TempTeams AS source
    ON target.teamId = source.teamId
    WHEN NOT MATCHED THEN
        INSERT (teamId)
        VALUES (source.teamId);
    
    DROP TABLE #TempTeams;
    """)
    
    print(f"Processed {len(team_ids)} teams")

def collect_teams(games_list):
    """
    Collect all unique team IDs from the games list.
    Returns a list of team IDs.
    """
    team_ids = set()
    
    for game in games_list:
        team_ids.add(game['homeTeam']['teamId'])
        team_ids.add(game['awayTeam']['teamId'])
    
    return list(team_ids)
  
  
def collect_team_stats(games_list):
    team_stats = []
    def home_away_stats_tuple(location): #location is 'home' or 'away'
        teamId = int(game[f'{location}Team']['teamId'])   
        gameId = int(game['gameId'])
        home = 1 if location == 'home' else 0  # Changed to 1/0 for SQL bit type
        if location=='away':
            if game[f'awayTeam']['score'] > game['homeTeam']['score']: 
                win =1 
            elif game[f'awayTeam']['score'] < game['homeTeam']['score']: 
                win =0
            else:
                win = None
        if location=='home':
            if game[f'awayTeam']['score'] > game['homeTeam']['score']: 
                win =0 
            elif game[f'awayTeam']['score'] < game['homeTeam']['score']:    
                win =1
            else:
                win = None
        score_dic = {'1': None, '2':None, '3':None, '4': None}
        for periods in game[f'{location}Team']['periods']:
            periodNum = str(periods['period'])
            score_dic[periodNum] = periods['score']
            
        stats = game[f'{location}Team'].get('statistics', {})
        
        minutes_str = stats.get('minutes', None)
        
        numMinutes = parse_duration(minutes_str)
        

        
        postgame_stats = game.get('postgameCharts', {}).get(f'{location}Team', {}).get('statistics', {})
        
        # Create tuple with all fields in correct order
        return (
            teamId,                  # 1
            gameId,                  # 2
            home,                    # 3 
            win,                     # 4
            stats.get('assists'),    # 5
            stats.get('blocks'),     # 6
            stats.get('fieldGoalsAttempted'),  # 7
            stats.get('fieldGoalsMade'),       # 8
            stats.get('fieldGoalsPercentage'), # 9
            stats.get('foulsPersonal'),        # 10
            stats.get('freeThrowsAttempted'),  # 11
            stats.get('freeThrowsMade'),       # 12
            stats.get('freeThrowsPercentage'), # 13
            numMinutes,              # 14
            stats.get('plusMinusPoints'),      # 15
            stats.get('points'),     # 16
            stats.get('reboundsDefensive'),    # 17
            stats.get('reboundsOffensive'),    # 18
            stats.get('reboundsTotal'),        # 19
            stats.get('steals'),     # 20
            stats.get('threePointersAttempted'), # 21
            stats.get('threePointersMade'),      # 22
            stats.get('threePointersPercentage'), # 23
            stats.get('turnovers'),  # 24
            score_dic['1'],          # 25 q1Points
            score_dic['2'],          # 26 q2Points
            score_dic['3'],          # 27 q3Points
            score_dic['4'],          # 28 q4Points
            postgame_stats.get('benchPoints'),      # 29
            postgame_stats.get('biggestLead'),      # 30
            postgame_stats.get('biggestScoringRun'), # 31
            postgame_stats.get('leadChanges'),       # 32
            postgame_stats.get('pointsFastBreak'),   # 33
            postgame_stats.get('pointsFromTurnovers'), # 34
            postgame_stats.get('pointsInThePaint'),    # 35
            postgame_stats.get('pointsSecondChance'),  # 36
            postgame_stats.get('timesTied'),          # 37
            game[f'{location}Team'].get('timeoutsRemaining'), # 38
            game[f'{location}Team'].get('teamWins'),     # 39
            game[f'{location}Team'].get('teamLosses')    # 40
        )
    
    for game in games_list:
        home_tuple = home_away_stats_tuple('home')
        away_tuple = home_away_stats_tuple('away')
        team_stats.extend([home_tuple, away_tuple])
    return team_stats
 
 
def insert_team_stats(cursor, team_stats):
    if not team_stats:
        return
        
    # First create the view if it doesn't exist
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.views WHERE name = 'TeamStatisticsNoCoach')
    BEGIN
        EXEC('
        CREATE VIEW TeamStatisticsNoCoach AS
        SELECT 
            teamId,
            gameId,
            home,
            win,
            assists,
            blocks,
            fieldGoalsAttempted,
            fieldGoalsMade,
            fieldGoalsPercentage,
            foulsPersonal,
            freeThrowsAttempted,
            freeThrowsMade,
            freeThrowsPercentage,
            numMinutes,
            plusMinusPoints,
            points,
            reboundsDefensive,
            reboundsOffensive,
            reboundsTotal,
            steals,
            threePointersAttempted,
            threePointersMade,
            threePointersPercentage,
            turnovers,
            q1Points,
            q2Points,
            q3Points,
            q4Points,
            benchPoints,
            biggestLead,
            biggestScoringRun,
            leadChanges,
            pointsFastBreak,
            pointsFromTurnovers,
            pointsInThePaint,
            pointsSecondChance,
            timesTied,
            timeoutsRemaining,
            seasonWins,
            seasonLosses
        FROM TeamStatistics
        ')
    END
    """)
        
    # Create temporary table matching the view structure
    cursor.execute("""
    CREATE TABLE #TempTeamStats (
        teamId INT,
        gameId INT,
        home BIT,
        win BIT,
        assists INT,
        blocks INT,
        fieldGoalsAttempted INT,
        fieldGoalsMade INT,
        fieldGoalsPercentage FLOAT,
        foulsPersonal INT,
        freeThrowsAttempted INT,
        freeThrowsMade INT,
        freeThrowsPercentage FLOAT,
        numMinutes INT,
        plusMinusPoints INT,
        points INT,
        reboundsDefensive INT,
        reboundsOffensive INT,
        reboundsTotal INT,
        steals INT,
        threePointersAttempted INT,
        threePointersMade INT,
        threePointersPercentage FLOAT,
        turnovers INT,
        q1Points INT,
        q2Points INT,
        q3Points INT,
        q4Points INT,
        benchPoints INT,
        biggestLead INT,
        biggestScoringRun INT,
        leadChanges INT,
        pointsFastBreak INT,
        pointsFromTurnovers INT,
        pointsInThePaint INT,
        pointsSecondChance INT,
        timesTied INT,
        timeoutsRemaining INT,
        seasonWins INT,
        seasonLosses INT,
        PRIMARY KEY (teamId, gameId)
    )
    """)
    
    # Insert into temp table
    insert_temp_sql = """
    INSERT INTO #TempTeamStats VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    """
    cursor.executemany(insert_temp_sql, team_stats)
    
    # Merge from temp table to the view
    cursor.execute("""
    MERGE INTO TeamStatisticsNoCoach AS target
    USING #TempTeamStats AS source
    ON target.teamId = source.teamId AND target.gameId = source.gameId
    WHEN MATCHED THEN
        UPDATE SET
            home = source.home,
            win = source.win,
            assists = source.assists,
            blocks = source.blocks,
            fieldGoalsAttempted = source.fieldGoalsAttempted,
            fieldGoalsMade = source.fieldGoalsMade,
            fieldGoalsPercentage = source.fieldGoalsPercentage,
            foulsPersonal = source.foulsPersonal,
            freeThrowsAttempted = source.freeThrowsAttempted,
            freeThrowsMade = source.freeThrowsMade,
            freeThrowsPercentage = source.freeThrowsPercentage,
            numMinutes = source.numMinutes,
            plusMinusPoints = source.plusMinusPoints,
            points = source.points,
            reboundsDefensive = source.reboundsDefensive,
            reboundsOffensive = source.reboundsOffensive,
            reboundsTotal = source.reboundsTotal,
            steals = source.steals,
            threePointersAttempted = source.threePointersAttempted,
            threePointersMade = source.threePointersMade,
            threePointersPercentage = source.threePointersPercentage,
            turnovers = source.turnovers,
            q1Points = source.q1Points,
            q2Points = source.q2Points,
            q3Points = source.q3Points,
            q4Points = source.q4Points,
            benchPoints = source.benchPoints,
            biggestLead = source.biggestLead,
            biggestScoringRun = source.biggestScoringRun,
            leadChanges = source.leadChanges,
            pointsFastBreak = source.pointsFastBreak,
            pointsFromTurnovers = source.pointsFromTurnovers,
            pointsInThePaint = source.pointsInThePaint,
            pointsSecondChance = source.pointsSecondChance,
            timesTied = source.timesTied,
            timeoutsRemaining = source.timeoutsRemaining,
            seasonWins = source.seasonWins,
            seasonLosses = source.seasonLosses
    WHEN NOT MATCHED THEN
        INSERT VALUES (
            source.teamId, source.gameId, source.home, source.win,
            source.assists, source.blocks, source.fieldGoalsAttempted,
            source.fieldGoalsMade, source.fieldGoalsPercentage,
            source.foulsPersonal, source.freeThrowsAttempted,
            source.freeThrowsMade, source.freeThrowsPercentage,
            source.numMinutes, source.plusMinusPoints, source.points,
            source.reboundsDefensive, source.reboundsOffensive,
            source.reboundsTotal, source.steals,
            source.threePointersAttempted, source.threePointersMade,
            source.threePointersPercentage, source.turnovers,
            source.q1Points, source.q2Points, source.q3Points,
            source.q4Points, source.benchPoints, source.biggestLead,
            source.biggestScoringRun, source.leadChanges,
            source.pointsFastBreak, source.pointsFromTurnovers,
            source.pointsInThePaint, source.pointsSecondChance,
            source.timesTied, source.timeoutsRemaining,
            source.seasonWins, source.seasonLosses
        );
        
    DROP TABLE #TempTeamStats;
    """)



def collect_player_stats(games_list):
    def home_away_player_stats(player, gameId, location): #location is 'home' or 'away'
        personId = int(player['personId']) 
        teamId = game[f'{location}Team']['teamId']
        
        # Safely get statistics with default empty dict
        stats = player.get('statistics', {})
        
        # Get all stats with None as default
        assists = stats.get('assists', None)
        blocks = stats.get('blocks', None)
        fieldGoalsAttempted = stats.get('fieldGoalsAttempted', None)
        fieldGoalsMade = stats.get('fieldGoalsMade', None)
        fieldGoalsPercentage = stats.get('fieldGoalsPercentage', None)
        foulsPersonal = stats.get('foulsPersonal', None)
        freeThrowsAttempted = stats.get('freeThrowsAttempted', None)
        freeThrowsMade = stats.get('freeThrowsMade', None)
        freeThrowsPercentage = stats.get('freeThrowsPercentage', None)
        
        # Handle minutes - should be decimal(6,3)
        minutes_str = stats.get('minutes', None)
        if minutes_str:
            try:
                parts = minutes_str.split(':')
                minutes = int(parts[0])
                seconds = int(parts[1]) if len(parts) > 1 else 0
                numMinutes = float(f"{minutes}.{seconds:02d}")
            except (ValueError, AttributeError, IndexError):
                numMinutes = None
        else:
            numMinutes = None
            
        # Get remaining stats with None defaults
        plusMinusPoints = stats.get('plusMinusPoints', None)
        points = stats.get('points', None)
        reboundsDefensive = stats.get('reboundsDefensive', None)
        reboundsOffensive = stats.get('reboundsOffensive', None)
        reboundsTotal = stats.get('reboundsTotal', None)
        steals = stats.get('steals', None)
        threePointersAttempted = stats.get('threePointersAttempted', None)
        threePointersMade = stats.get('threePointersMade', None)
        threePointersPercentage = stats.get('threePointersPercentage', None)
        turnovers = stats.get('turnovers', None)
        
        return tuple([
            personId,                    # int
            str(gameId),                # nvarchar(40)
            teamId,                     # int
            assists,                    # int
            blocks,                     # int
            fieldGoalsAttempted,        # int
            fieldGoalsMade,             # int
            fieldGoalsPercentage,       # decimal(6,3)
            foulsPersonal,              # int
            freeThrowsAttempted,        # int
            freeThrowsMade,             # int
            freeThrowsPercentage,       # decimal(6,3)
            numMinutes,                 # decimal(6,3)
            plusMinusPoints,            # decimal(6,3)
            points,                     # int
            reboundsDefensive,          # int
            reboundsOffensive,          # int
            reboundsTotal,              # int
            steals,                     # int
            threePointersAttempted,     # int
            threePointersMade,          # int
            threePointersPercentage,    # decimal(6,3)
            turnovers                   # int
        ])
    
    players_stats = []
    
    for game in games_list:
        gameId = game['gameId']  # No need to convert to int, keep as string
        
        # Process home team players
        for player in game.get('homeTeam', {}).get('players', []):
            try:
                player_tuple = home_away_player_stats(player, gameId, 'home')
                players_stats.append(player_tuple)
            except Exception as e:
                print(f"Error processing home player {player.get('personId', 'unknown')}: {str(e)}")
                continue
                
        # Process away team players
        for player in game.get('awayTeam', {}).get('players', []):
            try:
                player_tuple = home_away_player_stats(player, gameId, 'away')
                players_stats.append(player_tuple)
            except Exception as e:
                print(f"Error processing away player {player.get('personId', 'unknown')}: {str(e)}")
                continue
            
    return players_stats
    
def update_NBA_db(conn, cursor, when='last_three_days'):
    """
    Updates NBA database with new game data and player statistics.
    Returns None on success, unfound_games list if game retrieval fails,
    or games_list if database updates fail.
    
    Args:
        conn: Database connection
        cursor: Database cursor
        when: Time period to check for new games
    """
    try:
        logger.info('Searching for new games...')
        unfound_games = find_new_games(cursor, when=when)
        
        logger.info('Retrieving games from NBA.com...')
        try:
            games_list = get_new_games(unfound_games)
        except Exception as e:
            logger.error(f'Failed to retrieve games from NBA.com: {str(e)}')
            return unfound_games

        try:
            # Update Players
            logger.info('Updating Players table...')
            all_players, _, _ = collect_all_players(games_list, conn)
            insert_players(cursor, all_players)
            conn.commit()

            # Update Teams
            logger.info('Updating Teams table...')
            teams = collect_teams(games_list)
            insert_teams(cursor, teams)
            conn.commit()

            # Update Games
            logger.info('Updating Games table...')
            games = collect_games(games_list)
            insert_games(cursor, games)
            conn.commit()

            # Update Team Statistics
            logger.info('Updating TeamStatistics table...')
            team_stats = collect_team_stats(games_list)
            insert_team_stats(cursor, team_stats)
            conn.commit()

            # Update Player Statistics
            logger.info('Updating PlayerStatistics table...')
            player_stats = collect_player_stats(games_list)
            insert_player_stats(cursor, player_stats)
            conn.commit()

            logger.info('Database updates completed successfully')
            return None

        except Exception as e:
            logger.error(f'Database update failed: {str(e)}')
            conn.rollback()
            return games_list

    except Exception as e:
        logger.error(f'An unexpected error occurred: {str(e)}')
        return games_list    


def lambda_handler(event, context):
    """
    Lambda handler for updating NBA database with new game data and statistics.
    """
    # Get configuration from environment variables
    INSTANCE_ID = os.getenv('EC2_INSTANCE_ID')
    if not INSTANCE_ID:
        logger.error("EC2_INSTANCE_ID environment variable not set")
        raise ValueError("EC2_INSTANCE_ID environment variable not set")

    ec2 = boto3.client('ec2')
    
    try:
        # Check and manage EC2 instance state
        response = ec2.describe_instances(InstanceIds=[INSTANCE_ID])
        state = response['Reservations'][0]['Instances'][0]['State']['Name']
        logger.info(f"Current instance state: {state}")
        
        # Handle instance state
        if state == 'stopped':
            logger.info("Starting stopped instance...")
            ec2.start_instances(InstanceIds=[INSTANCE_ID])
        elif state == 'running':
            logger.info("Rebooting running instance...")
            ec2.reboot_instances(InstanceIds=[INSTANCE_ID])
            time.sleep(5)
        else:
            logger.warning(f"Instance in unexpected state: {state}")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': f'Instance in unexpected state: {state}',
                    'timestamp': datetime.now().isoformat()
                })
            }

        # Wait for instance to be ready
        waiter = ec2.get_waiter('instance_running')
        waiter.wait(
            InstanceIds=[INSTANCE_ID],
            WaiterConfig={'Delay': 5, 'MaxAttempts': 40}
        )
        
        waiter = ec2.get_waiter('instance_status_ok')
        waiter.wait(
            InstanceIds=[INSTANCE_ID],
            WaiterConfig={'Delay': 5, 'MaxAttempts': 40}
        )

        logger.info("Starting Lambda execution")

        # Get database connection using the get_db_connection function
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Database connection failed',
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                })
            }

        # Execute update process
        try:
            result = update_NBA_db(conn=conn, cursor = cursor, when='last_three_days')
            
            if result is None:
                logger.info("Database update completed successfully")
                response_message = "Database update completed successfully"
                status_code = 200
            elif isinstance(result, set):
                logger.warning(f"Failed to retrieve games: {result}")
                response_message = f"Failed to retrieve {len(result)} games"
                status_code = 500
            else:
                logger.error("Database update failed")
                response_message = "Database update failed"
                status_code = 500

        except Exception as e:
            logger.error(f"Update process failed: {str(e)}")
            status_code = 500
            response_message = "Update process failed"
        
        finally:
            # Clean up database connections
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                logger.info("Database connection closed")

        # Emit event to EventBridge
        try:
            events_client = boto3.client('events')
            event_entry = {
                'Source': os.getenv('EVENT_SOURCE', 'custom.nbapipeline'),
                'DetailType': 'Lambda Completion',
                'Detail': json.dumps({'status': 'success' if status_code == 200 else 'failure'}),
                'EventBusName': os.getenv('EVENT_BUS', 'default')
            }
            
            logger.info(f"Attempting to emit event: {event_entry}")
            events_client.put_events(Entries=[event_entry])
            
        except Exception as event_error:
            logger.error(f"EventBridge error: {str(event_error)}")

        return {
            'statusCode': status_code,
            'body': json.dumps({
                'message': response_message,
                'timestamp': datetime.now().isoformat()
            })
        }

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'An error occurred',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
        }