/*
NBA Database Creation Script
Generated: 2025-01-08

Dependencies and Creation Order:
1. Base Tables (no dependencies):
   - Teams (referenced by many other tables)
   - Coaches (referenced by CoachHistory)
   - Players (referenced by PlayerStatistics)
2. Core Tables:
   - Games (references Teams)
   - TeamHistories (references Teams)
3. Relationship Tables:
   - CoachHistory (references Coaches, Teams)
   - PlayerStatistics (references Players, Games, Teams)
   - TeamStatistics (references Games, Teams)
4. Views:
   - GameTeams (schema-bound, references Games and TeamHistories)
   - Other views (reference GameTeams)
*/

-- Base Tables --

-- Teams: Core table for team information
-- No foreign key dependencies
CREATE TABLE [dbo].[Teams] (
    [teamId] int NOT NULL,
    [currentTeamName] nvarchar(50) NULL,
    [currentTeamCity] nvarchar(30) NULL,
    CONSTRAINT [PK__Teams__5ED7536A435034F8] PRIMARY KEY CLUSTERED ([teamId] ASC)
);

-- Coaches: Core table for coach information
-- No foreign key dependencies
CREATE TABLE [dbo].[Coaches] (
    [coachId] int NOT NULL,
    [firstName] nvarchar(50) NULL,
    [lastName] nvarchar(50) NULL,
    CONSTRAINT [PK__Coaches__6046CD9510F45C10] PRIMARY KEY CLUSTERED ([coachId] ASC)
);

-- Players: Core table for player information
-- No foreign key dependencies
CREATE TABLE [dbo].[Players] (
    [personId] int NOT NULL,
    [firstName] nvarchar(30) NULL,
    [lastName] nvarchar(30) NULL,
    [birthdate] date NULL,
    [school] nvarchar(50) NULL,
    [country] nvarchar(30) NULL,
    [height] int NULL,
    [bodyWeight] int NULL,
    [guard] bit NULL,
    [forward] bit NULL,
    [center] bit NULL,
    [draftYear] int NULL,
    [draftRound] int NULL,
    [draftNumber] int NULL,
    [dleague] bit NULL,
    CONSTRAINT [PK__Players__EC7D7D4DD2A6B464] PRIMARY KEY CLUSTERED ([personId] ASC)
);

-- Create index for player name lookups
CREATE NONCLUSTERED INDEX [IX_Players_Names] ON [dbo].[Players] 
(
    [personId] ASC
)
INCLUDE ([firstName], [lastName]);

-- Games: Core table tracking all games
-- References Teams table
CREATE TABLE [dbo].[Games] (
    [gameId] int NOT NULL,
    [gameDate] datetime NOT NULL,
    [gameDuration] nvarchar(10) NULL,
    [hometeamId] int NULL,
    [awayteamId] int NULL,
    [homeScore] int NULL,
    [awayScore] int NULL,
    [winner] int NULL,
    [arenaId] int NULL,
    [attendance] int NULL,
    [gameType] nvarchar(20) NULL,
    [tournamentRound] int NULL
);

-- Clustered index on gameDate for temporal ordering and efficient inserts
CREATE CLUSTERED INDEX [CIX_Games_GameDate] ON [dbo].[Games] 
(
    [gameDate] DESC
);

-- Primary Key (nonclustered since we cluster on date)
ALTER TABLE [dbo].[Games] 
ADD CONSTRAINT [PK_Games] PRIMARY KEY NONCLUSTERED ([gameId] ASC);

-- Indexes for common query patterns
CREATE NONCLUSTERED INDEX [IX_Games_DateId] ON [dbo].[Games] 
(
    [gameDate] DESC,
    [gameId] DESC,
    [hometeamId] ASC,
    [awayteamId] ASC,
    [winner] ASC,
    [homeScore] ASC,
    [awayScore] ASC
);

CREATE NONCLUSTERED INDEX [IX_Games_Teams] ON [dbo].[Games] 
(
    [gameDate] DESC,
    [winner] ASC,
    [homeScore] ASC,
    [awayScore] ASC,
    [hometeamId] ASC,
    [awayteamId] ASC
);

-- LeagueSchedule24_25: Schedule information for 2024-25 season
CREATE TABLE [dbo].[LeagueSchedule24_25] (
   [gameId] nvarchar(20) NOT NULL,
   [gameDateTimeEst] nvarchar(50) NULL,
   [gameDay] nvarchar(50) NULL,
   [arenaCity] nvarchar(50) NULL,
   [arenaState] nvarchar(50) NULL,
   [arenaName] nvarchar(50) NULL,
   [gameLabel] nvarchar(50) NULL,
   [gameSubLabel] nvarchar(50) NULL,
   [gameSubtype] nvarchar(50) NULL,
   [gameSequence] tinyint NULL,
   [seriesGameNumber] nvarchar(1) NULL,
   [seriesText] nvarchar(50) NULL,
   [weekNumber] tinyint NULL,
   [hometeamId] int NULL,
   [awayteamId] int NULL,
   CONSTRAINT [PK_leagueSchedule] PRIMARY KEY CLUSTERED ([gameId] ASC)
);

-- TeamHistories: Historical team information with date ranges
CREATE TABLE [dbo].[TeamHistories] (
   [teamId] int NOT NULL,
   [teamCity] nvarchar(50) NULL,
   [teamName] nvarchar(50) NULL,
   [teamAbbrev] nchar(5) NULL,
   [yearFounded] int NULL,
   [yearActiveTill] int NULL
);

-- Clustered index for temporal queries and team lookups
CREATE CLUSTERED INDEX [CIX_TeamHistories] ON [dbo].[TeamHistories]
(
   [teamId] ASC,
   [yearFounded] ASC,
   [yearActiveTill] ASC
);

CREATE NONCLUSTERED INDEX [IX_TeamHistories] ON [dbo].[TeamHistories]
(
   [teamId] ASC
);

-- Index optimized for team name/city lookups with date ranges
CREATE NONCLUSTERED INDEX [IX_TeamHistories_TeamId_Dates] ON [dbo].[TeamHistories]
(
   [teamId] ASC,
   [yearFounded] ASC,
   [yearActiveTill] ASC
)
INCLUDE ([teamCity], [teamName]);

-- Additional index for team info queries
CREATE NONCLUSTERED INDEX [IX_TeamHistories_TeamInfo] ON [dbo].[TeamHistories]
(
   [teamId] ASC,
   [yearFounded] ASC,
   [yearActiveTill] ASC
)
INCLUDE ([teamCity], [teamName]);

-- CoachHistory: Tracks coaching assignments over time
CREATE TABLE [dbo].[CoachHistory] (
   [id] int NOT NULL,
   [coachId] int NULL,
   [teamId] int NULL,
   [startDate] date NULL,
   [endDate] date NULL,
   [role] nvarchar(50) NULL,
   CONSTRAINT [PK__CoachHis__3213E83F60B47426] PRIMARY KEY CLUSTERED ([id] ASC)
);

-- PlayerStatistics: Game-level statistics for each player
CREATE TABLE [dbo].[PlayerStatistics] (
   [id] int NOT NULL,
   [personId] int NOT NULL,
   [gameId] int NOT NULL,
   [teamId] int NOT NULL,
   [assists] int NULL,
   [blocks] int NULL,
   [fieldGoalsAttempted] int NULL,
   [fieldGoalsMade] int NULL,
   [fieldGoalsPercentage] decimal NULL,
   [foulsPersonal] int NULL,
   [freeThrowsAttempted] int NULL,
   [freeThrowsMade] int NULL,
   [freeThrowsPercentage] decimal NULL,
   [numMinutes] int NULL,
   [plusMinusPoints] int NULL,
   [points] int NULL,
   [reboundsDefensive] int NULL,
   [reboundsOffensive] int NULL,
   [reboundsTotal] int NULL,
   [steals] int NULL,
   [threePointersAttempted] int NULL,
   [threePointersMade] int NULL,
   [threePointersPercentage] decimal NULL,
   [turnovers] int NULL,
   CONSTRAINT [PK_PlayerStatistics] PRIMARY KEY CLUSTERED (
       [gameId] DESC,
       [personId] ASC
   )
);

-- Index for team-based queries
CREATE NONCLUSTERED INDEX [IX_PlayerStatistics_Team] ON [dbo].[PlayerStatistics]
(
   [teamId] ASC,
   [gameId] DESC
);

-- TeamStatistics: Game-level statistics for each team
CREATE TABLE [dbo].[TeamStatistics] (
    [teamId] int NOT NULL,
    [gameId] int NOT NULL,
    [home] bit NULL,
    [win] bit NULL,
    [coachId] int NULL,
    [assists] int NULL,
    [blocks] int NULL,
    [fieldGoalsAttempted] int NULL,
    [fieldGoalsMade] int NULL,
    [fieldGoalsPercentage] decimal NULL,
    [foulsPersonal] int NULL,
    [freeThrowsAttempted] int NULL,
    [freeThrowsMade] int NULL,
    [freeThrowsPercentage] decimal NULL,
    [numMinutes] int NULL,
    [plusMinusPoints] int NULL,
    [points] int NULL,
    [reboundsDefensive] int NULL,
    [reboundsOffensive] int NULL,
    [reboundsTotal] int NULL,
    [steals] int NULL,
    [threePointersAttempted] int NULL,
    [threePointersMade] int NULL,
    [threePointersPercentage] decimal NULL,
    [turnovers] int NULL,
    [q1Points] int NULL,
    [q2Points] int NULL,
    [q3Points] int NULL,
    [q4Points] int NULL,
    [benchPoints] int NULL,
    [biggestLead] int NULL,
    [biggestScoringRun] int NULL,
    [leadChanges] int NULL,
    [pointsFastBreak] int NULL,
    [pointsFromTurnovers] int NULL,
    [pointsInThePaint] int NULL,
    [pointsSecondChance] int NULL,
    [timesTied] int NULL,
    [timeoutsRemaining] int NULL,
    [seasonWins] int NULL,
    [seasonLosses] int NULL,
    CONSTRAINT [PK_TeamStatistics] PRIMARY KEY CLUSTERED (
        [gameId] DESC,
        [teamId] ASC
    )
);

-- Index for team-based queries
CREATE NONCLUSTERED INDEX [IX_TeamStatistics_Team] ON [dbo].[TeamStatistics]
(
    [points] ASC,
    [home] ASC,
    [win] ASC,
    [teamId] ASC,
    [gameId] DESC
);

GO
-- Foreign Key Constraints

-- Games FKs
ALTER TABLE [dbo].[Games]
ADD CONSTRAINT [FK_Games_HomeTeam] FOREIGN KEY ([hometeamId]) 
REFERENCES [dbo].[Teams] ([teamId]);

ALTER TABLE [dbo].[Games]
ADD CONSTRAINT [FK_Games_AwayTeam] FOREIGN KEY ([awayteamId]) 
REFERENCES [dbo].[Teams] ([teamId]);

-- CoachHistory FKs
ALTER TABLE [dbo].[CoachHistory]
ADD CONSTRAINT [FK__CoachHist__coach__403A8C7D] FOREIGN KEY ([coachId])
REFERENCES [dbo].[Coaches] ([coachId]);

ALTER TABLE [dbo].[CoachHistory]
ADD CONSTRAINT [FK__CoachHist__teamI__412EB0B6] FOREIGN KEY ([teamId])
REFERENCES [dbo].[Teams] ([teamId]);

-- PlayerStatistics FKs
ALTER TABLE [dbo].[PlayerStatistics]
ADD CONSTRAINT [FK_PlayerStatistics_Players] FOREIGN KEY ([personId])
REFERENCES [dbo].[Players] ([personId]);

ALTER TABLE [dbo].[PlayerStatistics]
ADD CONSTRAINT [FK_PlayerStatistics_Games] FOREIGN KEY ([gameId])
REFERENCES [dbo].[Games] ([gameId]);

ALTER TABLE [dbo].[PlayerStatistics]
ADD CONSTRAINT [FK_PlayerStatistics_Teams] FOREIGN KEY ([teamId])
REFERENCES [dbo].[Teams] ([teamId]);

-- TeamHistories FK
ALTER TABLE [dbo].[TeamHistories]
ADD CONSTRAINT [FK_TeamHistories_Teams] FOREIGN KEY ([teamId])
REFERENCES [dbo].[Teams] ([teamId]);

-- TeamStatistics FKs
ALTER TABLE [dbo].[TeamStatistics]
ADD CONSTRAINT [FK_TeamStatistics_Games] FOREIGN KEY ([gameId])
REFERENCES [dbo].[Games] ([gameId]);

ALTER TABLE [dbo].[TeamStatistics]
ADD CONSTRAINT [FK_TeamStatistics_Teams] FOREIGN KEY ([teamId])
REFERENCES [dbo].[Teams] ([teamId]);

GO
-- Views
-- Note: Order is important due to dependencies between views

/*
GameTeams is a schema-bound view that acts as a base for other views
It joins Games with TeamHistories to get team names for both home and away teams
at the time the game was played
*/
CREATE VIEW [dbo].[GameTeams]
WITH SCHEMABINDING
AS
SELECT 
   G.gameId,
   G.gameDate,
   G.hometeamId,
   G.awayteamId,
   G.homeScore,
   G.awayScore,
   G.winner,
   HT.teamCity as hometeamCity,
   HT.teamName as hometeamName,
   AT.teamCity as awayteamCity,
   AT.teamName as awayteamName
FROM dbo.Games G
INNER JOIN dbo.TeamHistories HT
   ON G.hometeamid = HT.teamId 
   AND YEAR(G.gameDate) >= HT.yearFounded 
   AND YEAR(G.gameDate) <= HT.yearActiveTill
INNER JOIN dbo.TeamHistories AT
   ON G.awayteamid = AT.teamId
   AND YEAR(G.gameDate) >= AT.yearFounded 
   AND YEAR(G.gameDate) <= AT.yearActiveTill;
GO

/*
DetailedGames enriches the Games table with team names
from the GameTeams view
*/
CREATE VIEW [dbo].[DetailedGames] 
AS 
SELECT 
   G.gameId,
   G.gameDate,
   GT.hometeamCity,
   GT.hometeamName,
   G.hometeamId,
   GT.awayteamCity,
   GT.awayteamName,
   G.awayteamId,
   G.homeScore,
   G.awayScore,
   G.winner,
   G.arenaId,
   G.attendance,
   G.gameType,
   G.tournamentRound 
FROM Games G WITH (NOLOCK) 
INNER JOIN GameTeams GT WITH (NOLOCK)
   ON G.gameId = GT.gameId;
GO

/*
DetailedPlayerStatistics combines player statistics with player and team information
Includes logic to determine:
- Player's team name and city
- Opponent team name and city
- Whether player's team won
- Whether player's team was home team
*/
CREATE VIEW [dbo].[DetailedPlayerStatistics] 
AS 
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
FROM PlayerStatistics PS WITH (NOLOCK)
INNER JOIN Players P WITH (NOLOCK)
   ON PS.personId = P.personId 
INNER JOIN GameTeams GT WITH (NOLOCK)
   ON PS.gameId = GT.gameId;
GO

/*
DetailedTeamStatistics combines team statistics with team names and opponent information
Includes advanced statistics like quarter points, bench points, and various game metrics
*/
CREATE VIEW [dbo].[DetailedTeamStatistics] 
AS 
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
FROM TeamStatistics TS WITH (NOLOCK)
INNER JOIN GameTeams GT WITH (NOLOCK)
   ON TS.gameId = GT.gameId;
GO