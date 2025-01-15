# NBA Database Schema Documentation

This document details the design and optimization strategies implemented in the NBA statistics database. The schema is carefully structured to maintain historical accuracy while enabling efficient querying and updates.

## Table Structure and Dependencies

The database follows a deliberate creation order to handle dependencies correctly:

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

## Key Optimization Strategies

### Temporal Optimization
- Games table uses clustered index on gameDate DESC for efficient recent data access
- All game-related tables (PlayerStatistics, TeamStatistics) maintain reverse chronological ordering
- TeamHistories implements date range handling through yearFounded and yearActiveTill

### Index Design
1. Games Table:
```sql
CREATE CLUSTERED INDEX [CIX_Games_GameDate] ON [dbo].[Games] ([gameDate] DESC);
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
```

2. PlayerStatistics Table:
```sql
CONSTRAINT [PK_PlayerStatistics] PRIMARY KEY CLUSTERED 
(
    [gameId] DESC,
    [personId] ASC
)
```

3. TeamHistories Table:
```sql
CREATE CLUSTERED INDEX [CIX_TeamHistories] ON [dbo].[TeamHistories]
(
   [teamId] ASC,
   [yearFounded] ASC,
   [yearActiveTill] ASC
);
```

### View Optimization
The schema implements a hierarchical view structure for efficient data access:

1. Base View (Schema-bound):
```sql
CREATE VIEW [dbo].[GameTeams] WITH SCHEMABINDING
AS
SELECT 
   G.gameId,
   G.gameDate,
   -- Additional fields
FROM dbo.Games G
INNER JOIN dbo.TeamHistories HT
   ON G.hometeamid = HT.teamId 
   AND YEAR(G.gameDate) BETWEEN HT.yearFounded AND HT.yearActiveTill
```

2. Derived Views:
- DetailedGames
- DetailedPlayerStatistics
- DetailedTeamStatistics

### Performance Features

1. Temporal Data Management:
- Reverse chronological storage aligns with insertion patterns
- Efficient handling of team name/location changes
- Optimized for recent data access

2. Index Strategy:
- Clustered indexes on frequently accessed columns
- Strategic use of included columns
  

3. View Hierarchy:
- Schema-bound base view for team name resolution
- NOLOCK hints for high-concurrency reading
- Efficient handling of team/opponent logic

## Table Details

### Core Tables
1. Games
   - Clustered on gameDate DESC
   - Multiple indexes for different access patterns
   - Supports both date-based and team-based queries

2. PlayerStatistics
   - Clustered on (gameId DESC, personId)
   - Optimized for player performance queries
   - Efficient temporal access

3. TeamStatistics
   - Clustered on (gameId DESC, teamId)
   - Comprehensive game statistics
   - Includes advanced metrics

### Support Tables
1. TeamHistories
   - Tracks franchise changes over time
   - Efficiently handles team relocations
   - Optimized for date range queries

2. Players
   - Biographical and physical data
   - Indexed for name searches
   - Supports career tracking

This schema design enables efficient handling of both historical analysis and nightly updates while maintaining optimal query performance on resource-constrained infrastructure.
