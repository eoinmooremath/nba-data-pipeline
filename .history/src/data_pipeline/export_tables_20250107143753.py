import warnings
warnings.filterwarnings('ignore', category=UserWarning)
import pandas as pd
import pyodbc
import sys
import time
from datetime import datetime
import gc
from utils.db_utils import get_db_connection

def log_message(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - {message}")
    with open('nba_export.log', 'a') as f:
        f.write(f"{timestamp} - {message}\n")
        f.flush()

def setup_session_gameteams(conn):
    """Create persistent GameTeams table with minimal resource usage"""
    try:
        log_message("Starting GameTeams table setup...")
        
        # Cleanup first
        cleanup_query = """
        IF OBJECT_ID('tempdb..##GameTeams') IS NOT NULL
            DROP TABLE ##GameTeams;
        """
        conn.execute(cleanup_query)
        log_message("Cleaned up any existing GameTeams table")
        
        # Create and populate in smaller batches
        setup_query = """
        CREATE TABLE ##GameTeams (
            gameId bigint,
            gameDate datetime,
            hometeamId int,
            awayteamId int,
            homeScore int,
            awayScore int,
            winner int,
            hometeamCity varchar(50),
            hometeamName varchar(50),
            awayteamCity varchar(50),
            awayteamName varchar(50)
        );

        INSERT INTO ##GameTeams
        SELECT * FROM GameTeams WITH (NOLOCK)
        OPTION (MAXDOP 1);  -- Use single thread to reduce resource usage

        CREATE CLUSTERED INDEX IX_GameTeams_Date 
        ON ##GameTeams(gameDate DESC, gameId DESC)
        WITH (MAXDOP = 1, SORT_IN_TEMPDB = OFF);

        CREATE NONCLUSTERED INDEX IX_GameTeams_GameId
        ON ##GameTeams(gameId)
        WITH (MAXDOP = 1, SORT_IN_TEMPDB = OFF);
        """
        
        conn.execute(setup_query)
        log_message("Persistent GameTeams table created successfully")
        return True
        
    except Exception as e:
        log_message(f"Error creating persistent GameTeams table: {str(e)}")
        try:
            conn.execute("DROP TABLE IF EXISTS ##GameTeams;")
        except:
            pass
        return False
    
def export_view(conn, view_name, chunk_size=50000):
    """Export view data using yearly batches"""
    log_message(f"{'='*50}")
    log_message(f"Starting export of {view_name}")
    start_time = time.time()
    rows_processed = 0

    try:
        # Get date range for batching
        range_query = "SELECT MIN(gameDate) as min_date, MAX(gameDate) as max_date FROM ##GameTeams"
        date_range = pd.read_sql(range_query, conn)
        current_date = date_range['max_date'].iloc[0]
        min_date = date_range['min_date'].iloc[0]
        
        first_batch = True
        while current_date >= min_date:
            next_date = current_date - pd.DateOffset(years=14)
            
            if view_name == 'DetailedGames':
                query = f"""
                SELECT 
                    GT.gameId,
                    GT.gameDate,
                    GT.hometeamCity,
                    GT.hometeamName,
                    GT.hometeamId,
                    GT.awayteamCity,
                    GT.awayteamName,
                    GT.awayteamId,
                    GT.homeScore,
                    GT.awayScore,
                    GT.winner,
                    G.arenaId,
                    G.attendance,
                    G.gameType,
                    G.tournamentRound
                FROM ##GameTeams GT
                INNER JOIN Games G WITH (NOLOCK) ON GT.gameId = G.gameId
                WHERE GT.gameDate <= '{current_date}' AND GT.gameDate > '{next_date}'
                ORDER BY GT.gameDate DESC, GT.gameId DESC
                """
                
            elif view_name == 'DetailedTeamStatistics':
                query = f"""
                SELECT 
                    TS.gameId,
                    GT.gameDate,
                    CASE 
                        WHEN TS.home = 1 THEN GT.hometeamCity
                        ELSE GT.awayteamCity
                    END as teamCity,
                    CASE 
                        WHEN TS.home = 1 THEN GT.hometeamName
                        ELSE GT.awayteamName
                    END as teamName,
                    TS.teamId,
                    CASE 
                        WHEN TS.home = 1 THEN GT.awayteamCity
                        ELSE GT.hometeamCity
                    END as opponentTeamCity,
                    CASE 
                        WHEN TS.home = 1 THEN GT.awayteamName
                        ELSE GT.hometeamName
                    END as opponentTeamName,
                    CASE 
                        WHEN TS.home = 1 THEN GT.awayteamId
                        ELSE GT.hometeamId
                    END as opponentTeamId,
                    TS.home,
                    TS.win,
                    CASE 
                        WHEN TS.home = 1 THEN GT.homeScore
                        ELSE GT.awayScore
                    END as teamScore,
                    CASE 
                        WHEN TS.home = 1 THEN GT.awayScore
                        ELSE GT.homeScore
                    END as opponentScore,
                    TS.assists,
                    TS.blocks,
                    TS.steals,
                    TS.fieldGoalsAttempted,
                    TS.fieldGoalsMade,
                    TS.fieldGoalsPercentage,
                    TS.threePointersAttempted,
                    TS.threePointersMade,
                    TS.threePointersPercentage,
                    TS.freeThrowsAttempted,
                    TS.freeThrowsMade,
                    TS.freeThrowsPercentage,
                    TS.reboundsDefensive,
                    TS.reboundsOffensive,
                    TS.reboundsTotal,
                    TS.foulsPersonal,
                    TS.turnovers,
                    TS.plusMinusPoints,
                    TS.numMinutes,
                    TS.q1Points,
                    TS.q2Points,
                    TS.q3Points,
                    TS.q4Points,
                    TS.benchPoints,
                    TS.biggestLead,
                    TS.biggestScoringRun,
                    TS.leadChanges,
                    TS.pointsFastBreak,
                    TS.pointsFromTurnovers,
                    TS.pointsInThePaint,
                    TS.pointsSecondChance,
                    TS.timesTied,
                    TS.timeoutsRemaining,
                    TS.seasonWins,
                    TS.seasonLosses,
                    TS.coachId
                FROM ##GameTeams GT
                INNER JOIN TeamStatistics TS WITH (NOLOCK) ON GT.gameId = TS.gameId
                WHERE GT.gameDate <= '{current_date}' AND GT.gameDate > '{next_date}'
                ORDER BY GT.gameDate DESC, GT.gameId DESC
                """

            elif view_name == 'DetailedPlayerStatistics':
                query = f"""
                SELECT 
                    P.firstName,
                    P.lastName,
                    PS.personId,
                    PS.gameId,
                    GT.gameDate,
                    CASE 
                        WHEN PS.teamId = GT.hometeamId THEN GT.hometeamCity
                        ELSE GT.awayteamCity
                    END as playerteamCity,
                    CASE 
                        WHEN PS.teamId = GT.hometeamId THEN GT.hometeamName
                        ELSE GT.awayteamName
                    END as playerteamName,
                    CASE 
                        WHEN PS.teamId = GT.hometeamId THEN GT.awayteamCity
                        ELSE GT.hometeamCity
                    END as opponentteamCity,
                    CASE 
                        WHEN PS.teamId = GT.hometeamId THEN GT.awayteamName
                        ELSE GT.hometeamName
                    END as opponentteamName,
                    CASE 
                        WHEN PS.teamId = GT.winner THEN 1
                        ELSE 0
                    END as win,
                    CASE 
                        WHEN PS.teamId = GT.hometeamId THEN 1
                        ELSE 0
                    END as home,
                    PS.numMinutes,
                    PS.points,
                    PS.assists,
                    PS.blocks,
                    PS.steals,
                    PS.fieldGoalsAttempted,
                    PS.fieldGoalsMade,
                    PS.fieldGoalsPercentage,
                    PS.threePointersAttempted,
                    PS.threePointersMade,
                    PS.threePointersPercentage,
                    PS.freeThrowsAttempted,
                    PS.freeThrowsMade,
                    PS.freeThrowsPercentage,
                    PS.reboundsDefensive,
                    PS.reboundsOffensive,
                    PS.reboundsTotal,
                    PS.foulsPersonal,
                    PS.turnovers,
                    PS.plusMinusPoints
                FROM ##GameTeams GT
                INNER JOIN PlayerStatistics PS WITH (NOLOCK) ON GT.gameId = PS.gameId
                INNER JOIN Players P WITH (NOLOCK) ON PS.personId = P.personId
                WHERE GT.gameDate <= '{current_date}' AND GT.gameDate > '{next_date}'
                ORDER BY GT.gameDate DESC, GT.gameId DESC
                """

            chunk_df = pd.read_sql(query, conn)
            
            if len(chunk_df) > 0:
                if first_batch:
                    chunk_df.to_csv(f"{view_name.replace('Detailed', '')}.csv", index=False)
                    first_batch = False
                else:
                    chunk_df.to_csv(f"{view_name.replace('Detailed', '')}.csv", 
                                  mode='a', header=False, index=False)
            
                rows_processed += len(chunk_df)
                elapsed_time = time.time() - start_time
                log_message(
                    f"Processed {current_date.strftime('%Y-%m-%d')} to {next_date.strftime('%Y-%m-%d')}, "
                    f"{rows_processed} total rows. "
                    f"Speed: {rows_processed/elapsed_time:.1f} rows/sec. "
                    f"Time elapsed: {elapsed_time:.1f}s")
            
            current_date = next_date
            del chunk_df
            gc.collect()

        return True

    except Exception as e:
        log_message(f"Error in export: {str(e)}")
        return False
    
def export_regular_table(conn, table_name, chunk_size=10000):
    """Export regular tables without complex joins"""
    log_message(f"{'='*50}")
    log_message(f"Starting export of {table_name}")
    start_time = time.time()
    
    try:
        count_query = f"SELECT COUNT(*) as count FROM {table_name}"
        total_rows = pd.read_sql(count_query, conn).iloc[0]['count']
        log_message(f"Total rows to process: {total_rows}")

        if total_rows == 0:
            log_message(f"No rows to process for {table_name}")
            return True

        # Simple batch processing for regular tables
        for offset in range(0, total_rows, chunk_size):
            query = f"""
            SELECT *
            FROM {table_name}
            ORDER BY (SELECT NULL)
            OFFSET {offset} ROWS
            FETCH NEXT {chunk_size} ROWS ONLY
            """
            
            chunk_df = pd.read_sql(query, conn)
            
            if len(chunk_df) == 0:
                break
                
            if offset == 0:
                chunk_df.to_csv(f"{table_name}.csv", index=False)
            else:
                chunk_df.to_csv(f"{table_name}.csv", mode='a', header=False, index=False)
            
            rows_processed = offset + len(chunk_df)
            elapsed_time = time.time() - start_time
            log_message(f"Processed {rows_processed}/{total_rows} rows for {table_name}. "
                       f"Elapsed time: {elapsed_time:.2f} seconds")
            
            del chunk_df
            gc.collect()

        return True

    except Exception as e:
        log_message(f"Error processing table {table_name}: {str(e)}")
        return False

def main():
    try:
        conn = get_db_connection()

        # Create persistent GameTeams table for the session
        if not setup_session_gameteams(conn):
            raise Exception("Failed to create persistent GameTeams table")

        # Export views with smaller chunk size for DetailedPlayerStatistics
        views = ['DetailedGames', 'DetailedPlayerStatistics', 'DetailedTeamStatistics']
        for view in views:
            chunk_size = 25000 if view == 'DetailedPlayerStatistics' else 50000
            if not export_view(conn, view, chunk_size):
                log_message(f"Failed to export view: {view}")
                
        # Export other tables
        other_tables = ['Players', 'CommonPlayerInfo', 'TeamHistories', 'LeagueSchedule24_25','Arenas', 'Coaches']
        for table in other_tables:
            if not export_regular_table(conn, table):
                log_message(f"Failed to export table: {table}")

    except Exception as e:
        log_message(f"Fatal error: {str(e)}")
        if conn:
            try:
                conn.execute("DROP TABLE IF EXISTS ##GameTeams;")
            except:
                pass
        sys.exit(1)
        
    finally:
        if 'conn' in locals():
            try:
                conn.execute("DROP TABLE IF EXISTS ##GameTeams;")
                conn.close()
            except:
                pass
            log_message("Database connection closed")

if __name__ == "__main__":
    main()
