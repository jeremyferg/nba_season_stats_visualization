USE nba_stats;

-- lineup info
SELECT lineups.* FROM lineups
INNER JOIN team_info ON team_info.team_id = lineups.team_id 
WHERE team_info.team_name = 'Bulls' AND gp >= 5 AND minutes >= 10
ORDER BY lineups.minutes DESC;

-- total N
SELECT 
	player_info.player_id,
	player_info.first_name, 
	player_info.last_name, 
    SUM(gp) AS total_gp,
    SUM(gs) AS total_gs,
    SUM(minutes) AS total_minutes,
    SUM(pts) AS total_pts,
    SUM(reb) AS total_reb,
    SUM(ast) AS total_ast
FROM player_info
INNER JOIN player_seasons ON player_seasons.player_id = player_info.player_id
INNER JOIN team_info ON team_info.team_id = player_seasons.team_id
WHERE team_info.team_name = 'Bulls' AND
	player_seasons.season_id IN ('2014-15', '2015-16', '2016-17', '2017-18', '2018-19',
								 '2019-20', '2020-21', '2021-22', '2022-23', '2023-24', '2024-25')
GROUP BY player_info.player_id, player_info.first_name, player_info.last_name;

-- Bulls home games
-- BUlls' team ID = 1610612741
SELECT * FROM team_scoreboards
INNER JOIN games ON games.game_id = team_scoreboards.game_id
WHERE games.home_team_id = 1610612741 AND team_scoreboards.team_id = 1610612741;
SELECT * FROM games;

-- Bulls natinally televised home games
-- national broadcasting data doesnt start until 2017-18 season
SELECT 
	team_scoreboards.*, 
    games.home_team_pts, 
    games.visitor_team_pts, 
    games.outcome, 
    broadcasters.broadcaster_name 
FROM team_scoreboards
INNER JOIN games ON games.game_id = team_scoreboards.game_id
INNER JOIN broadcasters ON broadcasters.broadcaster_id = games.nat_broadcaster_id
WHERE 
	games.home_team_id = 1610612741 AND 
    team_scoreboards.team_id = 1610612741 AND
    games.nat_broadcaster_id IS NOT NULL;

-- Bulls 2023-24 season
SELECT * FROM team_scoreboards
WHERE 
	game_date BETWEEN '2023-10-1' AND '2024-06-30' AND
    team_scoreboards.team_id = 1610612741
ORDER BY game_date;

-- W-L by arena
-- arena records start in 2017-18 season
SELECT
	team_info.team_name,
	arenas.arena_name,
    arenas.arena_city,
    arenas.arena_state,
	SUM(CASE WHEN team_scoreboards.winlose = 'W' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN team_scoreboards.winlose = 'L' THEN 1 ELSE 0 END) AS losses,
    ROUND(
        SUM(CASE WHEN team_scoreboards.winlose = 'W' THEN 1 ELSE 0 END) /
        SUM(CASE WHEN team_scoreboards.winlose IN ('W','L') THEN 1 ELSE 0 END),
        3
    ) AS win_pct
FROM team_scoreboards
INNER JOIN games ON games.game_id = team_scoreboards.game_id
INNER JOIN arenas ON arenas.arena_id = games.arena_id
INNER JOIN team_info ON team_info.team_id = games.home_team_id
WHERE team_scoreboards.team_id = 1610612741 AND arenas.arena_id != 40
GROUP BY arenas.arena_id, arenas.arena_name, games.home_team_id, team_info.team_name
ORDER BY wins DESC;

-- more completed version of arenas 
SELECT
	team_info.team_name,
	SUM(CASE WHEN team_scoreboards.winlose = 'W' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN team_scoreboards.winlose = 'L' THEN 1 ELSE 0 END) AS losses,
    ROUND(
        SUM(CASE WHEN team_scoreboards.winlose = 'W' THEN 1 ELSE 0 END) /
        SUM(CASE WHEN team_scoreboards.winlose IN ('W','L') THEN 1 ELSE 0 END),
        3
    ) AS win_pct
FROM team_scoreboards
INNER JOIN games ON games.game_id = team_scoreboards.game_id
INNER JOIN arenas ON arenas.arena_id = games.arena_id
INNER JOIN team_info ON team_info.team_id = games.home_team_id
WHERE team_scoreboards.team_id = 1610612741 AND arenas.arena_id != 40
GROUP BY team_info.team_name
ORDER BY wins DESC;

-- total wins 
SELECT SUM(w) AS total_w, SUM(l) AS total_l, ROUND(AVG(win_pct), 3) AS avg_win_pct FROM team_seasons
WHERE team_id = 1610612741;

-- player stats by game 2023-24 season
SELECT player_scoreboards.* FROM team_scoreboards
INNER JOIN player_scoreboards ON (player_scoreboards.game_id, player_scoreboards.winlose) = (team_scoreboards.game_id, team_scoreboards.winlose)
WHERE team_scoreboards.game_date BETWEEN '2023-10-1' AND '2024-06-30' AND speed != 0 AND
	  team_id = 1610612741;

-- Bulls player awards (error in final MVP column...)
SELECT 
	player_info.player_id,
	player_info.first_name, 
	player_info.last_name, 
	team_info.team_name,
	player_awards.season_id, 
	awards.award_name 
FROM player_awards
INNER JOIN player_info ON player_info.player_id = player_awards.player_id
INNER JOIN player_seasons 
    ON player_seasons.player_id = player_awards.player_id
	AND player_seasons.season_id = player_awards.season_id
INNER JOIN team_info ON team_info.team_id = player_seasons.team_id
INNER JOIN awards ON awards.award_id = player_awards.award_id
WHERE player_awards.season_id IN ('2014-15', '2015-16', '2016-17', '2017-18', '2018-19',
								   '2019-20', '2020-21', '2021-22', '2022-23', '2023-24', '2024-25') AND
team_name != 'Trade' AND
team_info.team_id = 1610612741
ORDER BY player_awards.season_id, player_info.last_name, player_info.first_name;

-- SELECT * FROM player_awards
-- INNER JOIN awards ON awards.award_id = player_awards.award_id
-- WHERE player_awards.player_id = 1629632;