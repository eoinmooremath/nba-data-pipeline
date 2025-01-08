# NBA Database Schema Documentation

This document details the architecture and optimization strategies of the NBA statistical database, which houses every box score since 1946 and updates nightly. The schema is designed to balance historical completeness with query performance, especially for common access patterns in basketball analytics.

## Temporal Optimization Strategy

A key architectural decision in this database is storing and accessing data in reverse chronological order (newest first). This design choice provides several benefits:

1. Natural alignment with data insertion patterns:
   - New games are added nightly through the automated pipeline
   - Insertions happen at the "front" of the indexes where the newest data lives
   - Reduces index fragmentation from regular updates

2. Optimized for common access patterns:
   - Most queries focus on recent games and current season statistics
   - Reverse chronological indexes eliminate the need for expensive sort operations
   - Critical for performance on resource-constrained t3.micro instances

3. Efficient CSV generation:
   - Export processes naturally produce newest-first ordered data
   - Reduces memory requirements during file generation
   - Minimizes sorting operations on limited EC2 resources

The physical storage order is implemented through carefully designed descending indexes on date and game ID fields across the core tables.

## Core Tables Design

### PlayerStatistics
The heart of the database, optimized for efficient game-by-game player statistics retrieval:
```sql
CREATE TABLE PlayerStatistics (
    personId INT,
    gameId INT,
    teamId INT,
    assists INT,
    blocks INT,
    fieldGoalsAttempted INT,
    fieldGoalsMade INT,
    fieldGoalsPercentage DECIMAL(6,3),
    -- Additional statistics columns
    CONSTRAINT PK_PlayerStatistics PRIMARY KEY CLUSTERED (gameId, personId)
)

-- Index optimized for team-based queries
CREATE NONCLUSTERED INDEX IX_PlayerStatistics_Team 
ON PlayerStatistics (teamId, gameId)
```

Key optimization decisions:
- Clustered index on (gameId, personId) optimizes the most common query pattern: retrieving player performances from specific games
- Strategic nonclustered index on teamId supports efficient team roster analysis
- Decimal(6,3) for percentage fields balances precision with storage efficiency

### Games
Central reference table linking teams, scores, and game metadata:
```sql
CREATE TABLE Games (
    gameId INT PRIMARY KEY NONCLUSTERED,
    gameDate DATETIME,
    hometeamId INT,
    awayteamId INT,
    homeScore INT,
    awayScore INT,
    winner INT,
    -- Additional metadata columns
)

-- Optimized for date-based queries, newest first
CREATE NONCLUSTERED INDEX IX_Games_DateId ON Games 
    (gameDate DESC, gameId DESC, hometeamId, awayteamId, winner, homeScore, awayScore)

-- Optimized for team-based queries
CREATE NONCLUSTERED INDEX IX_Games_Teams ON Games 
    (gameDate, winner, homeScore, awayScore, hometeamId, awayteamId)
```

Index strategy:
- Date-based index supports efficient historical queries
- Team-based index optimizes season record calculations
- Intentionally nonclustered primary key allows for flexible query patterns

### TeamHistories
Tracks franchise changes and relocations with temporal support:
```sql
CREATE TABLE TeamHistories (
    teamId INT,
    teamCity NVARCHAR(100),
    teamName NVARCHAR(100),
    yearFounded INT,
    yearActiveTill INT,
    CONSTRAINT CIX_TeamHistories PRIMARY KEY CLUSTERED 
        (teamId, yearFounded, yearActiveTill)
)
```

Temporal tracking:
- Clustered index supports efficient historical lookups
- Maintains accurate team names and locations throughout NBA history

## Optimized Views

### GameTeams
A schema-bound view that optimizes common game data retrieval:
```sql
CREATE VIEW GameTeams WITH SCHEMABINDING AS
SELECT 
    G.gameId,
    G.gameDate,
    G.hometeamId,
    G.awayteamId,
    HT.teamCity as hometeamCity,
    HT.teamName as hometeamName,
    AT.teamCity as awayteamCity,
    AT.teamName as awayteamName
FROM dbo.Games G
INNER JOIN dbo.TeamHistories HT 
    ON G.hometeamid = HT.teamId 
    AND YEAR(G.gameDate) BETWEEN HT.yearFounded AND HT.yearActiveTill
INNER JOIN dbo.TeamHistories AT 
    ON G.awayteamid = AT.teamId
    AND YEAR(G.gameDate) BETWEEN AT.yearFounded AND AT.yearActiveTill
```

Key features:
- SCHEMABINDING ensures data integrity
- Temporal joins maintain historical accuracy
- Precomputed common joins improve query performance

### DetailedPlayerStatistics
Comprehensive player statistics view with team context:
```sql
CREATE VIEW DetailedPlayerStatistics AS
SELECT 
    P.firstName,
    P.lastName,
    PS.personId,
    PS.gameId,
    GT.gameDate,
    -- Additional calculated columns
FROM PlayerStatistics PS WITH (NOLOCK)
INNER JOIN Players P WITH (NOLOCK) 
    ON PS.personId = P.personId
INNER JOIN GameTeams GT WITH (NOLOCK)
    ON PS.gameId = GT.gameId
```

Optimization decisions:
- NOLOCK hints for improved read performance
- Leverages the GameTeams view for efficient team name resolution
- Calculates common basketball metrics at the view level

## Performance Considerations

The schema incorporates several key performance optimizations:

1. Strategic Index Placement:
   - Clustered indexes on most frequently accessed columns
   - Covering indexes for common query patterns
   - Balanced index strategy to optimize both reads and writes

2. Temporal Data Management:
   - Efficient handling of team relocations and name changes
   - Historical accuracy maintained through careful join conditions
   - Date-based indexing for timeline queries

3. View Optimization:
   - Schema binding where appropriate
   - Strategic use of NOLOCK hints
   - Precomputed common joins and calculations

4. Data Type Selection:
   - INT for IDs and statistical counts
   - DECIMAL(6,3) for percentages
   - NVARCHAR for variable-length text fields
   - Appropriate length constraints for all string columns

## Maintenance Considerations

The schema is designed for nightly updates with minimal performance impact:
- Indexes optimized for batch inserts of new games
- Efficient update paths for player and team statistics
- Minimal index fragmentation from regular operations

This design supports both efficient querying of historical data and seamless integration of nightly updates from ongoing NBA games.