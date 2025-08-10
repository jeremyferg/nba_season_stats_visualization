--------------------------------------
--------------------------------------
-----                            -----
-----   create_database_tables   -----
-----                            -----
--------------------------------------
--------------------------------------

CREATE DATABASE nba_stats;
USE nba_stats;

-----------------------------------------------------------------------------------------------------------------------------

---------------------
---               ---
---  INFO_TABLES  ---
---               ---
---------------------

CREATE TABLE player_info(
    player_id INT UNSIGNED PRIMARY KEY NOT NULL,
    first_name VARCHAR(30),
    last_name VARCHAR(30),
    birthdate DATE,
    school VARCHAR(50),
    country VARCHAR(50),
    height VARCHAR(4),
    weight SMALLINT UNSIGNED,
    season_exp TINYINT UNSIGNED,
    jersey VARCHAR(2),
    position VARCHAR(20),
    from_year SMALLINT UNSIGNED,
    to_year SMALLINT UNSIGNED,
    gleague CHAR(1),
    draft_year VARCHAR(10),
    draft_round VARCHAR(10),
    draft_num VARCHAR(10),
    nba_75 CHAR(1)
);

CREATE TABLE team_info(
    team_id INT UNSIGNED PRIMARY KEY NOT NULL,
    team_city VARCHAR(20),
    team_name VARCHAR(20),
    start_year SMALLINT UNSIGNED,
    end_year SMALLINT UNSIGNED,
    years TINYINT UNSIGNED,
    games SMALLINT UNSIGNED,
    wins SMALLINT UNSIGNED,
    losses SMALLINT UNSIGNED,
    win_pct DECIMAL(4,3),
    playoff_berths TINYINT UNSIGNED,
    div_titles TINYINT UNSIGNED,
    conf_titles TINYINT UNSIGNED,
    championships TINYINT UNSIGNED 
);

-----------------------------------------------------------------------------------------------------------------------------

---------------------
---               ---
---  GAME_TABLES  ---
---               ---
---------------------

CREATE TABLE arenas(
    arena_id TINYINT UNSIGNED PRIMARY KEY NOT NULL,
    arena_name VARCHAR(100),
    arena_city VARCHAR(100),
    arena_state VARCHAR(2)
);

CREATE TABLE broadcasters(
    broadcaster_id INT UNSIGNED PRIMARY KEY NOT NULL,
    broadcaster_name VARCHAR(100)
);

CREATE TABLE games(
    game_id INT UNSIGNED PRIMARY KEY NOT NULL,
    game_date DATE,
    home_team_id INT UNSIGNED NOT NULL,
    visitor_team_id INT UNSIGNED NOT NULL,
    arena_id TINYINT UNSIGNED NOT NULL,
    nat_broadcaster_id INT UNSIGNED,
    home_broadcaster_id INT UNSIGNED,
    visitor_broadcaster_id INT UNSIGNED,
    home_team_pts TINYINT UNSIGNED,
    visitor_team_pts TINYINT UNSIGNED,
    outcome VARCHAR(10),
    FOREIGN KEY (arena_id) REFERENCES arenas(arena_id),
    FOREIGN KEY (nat_broadcaster_id) REFERENCES broadcasters(broadcaster_id),
    FOREIGN KEY (home_broadcaster_id) REFERENCES broadcasters(broadcaster_id),
    FOREIGN KEY (visitor_broadcaster_id) REFERENCES broadcasters(broadcaster_id)
);

-----------------------------------------------------------------------------------------------------------------------------

-----------------------
---                 ---
---  PLAYER_TABLES  ---
---                 ---
-----------------------

CREATE TABLE player_career(
    player_id INT UNSIGNED NOT NULL,
    team_id INT UNSIGNED NOT NULL,
    gp SMALLINT UNSIGNED,
    gs SMALLINT UNSIGNED,
    num_years TINYINT UNSIGNED,
    minutes INT UNSIGNED,
    fgm SMALLINT UNSIGNED,
    fga SMALLINT UNSIGNED,
    fg_pct DECIMAL(4,3),
    fg3m SMALLINT UNSIGNED,
    fg3a SMALLINT UNSIGNED,
    fg3_pct DECIMAL(4,3),
    ftm SMALLINT UNSIGNED,
    fta SMALLINT UNSIGNED,
    ft_pct DECIMAL(4,3),
    oreb SMALLINT UNSIGNED,
    dreb SMALLINT UNSIGNED,
    reb SMALLINT UNSIGNED,
    ast SMALLINT UNSIGNED,
    stl SMALLINT UNSIGNED,
    blk SMALLINT UNSIGNED,
    tov SMALLINT UNSIGNED,
    pf SMALLINT UNSIGNED,
    pts SMALLINT UNSIGNED,
    PRIMARY KEY (player_id, team_id),
    FOREIGN KEY (player_id) REFERENCES player_info(player_id),
    FOREIGN KEY (team_id) REFERENCES team_info(team_id)
);

CREATE TABLE player_seasons(
    player_id INT UNSIGNED NOT NULL,
    season_id VARCHAR(10) NOT NULL,
    team_id INT UNSIGNED NOT NULL,
    gp SMALLINT UNSIGNED,
    gs SMALLINT UNSIGNED,
    minutes INT UNSIGNED,
    fgm SMALLINT UNSIGNED,
    fga SMALLINT UNSIGNED,
    fg_pct DECIMAL(4,3),
    fg3m SMALLINT UNSIGNED,
    fg3a SMALLINT UNSIGNED,
    fg3_pct DECIMAL(4,3),
    ftm SMALLINT UNSIGNED,
    fta SMALLINT UNSIGNED,
    ft_pct DECIMAL(4,3),
    oreb SMALLINT UNSIGNED,
    dreb SMALLINT UNSIGNED,
    reb SMALLINT UNSIGNED,
    ast SMALLINT UNSIGNED,
    stl SMALLINT UNSIGNED,
    blk SMALLINT UNSIGNED,
    tov SMALLINT UNSIGNED,
    pf SMALLINT UNSIGNED,
    pts SMALLINT UNSIGNED,
    PRIMARY KEY (player_id, season_id, team_id),
    FOREIGN KEY (player_id) REFERENCES player_info(player_id),
    FOREIGN KEY (team_id) REFERENCES team_info(team_id)
);

CREATE TABLE awards(
    award_id TINYINT UNSIGNED PRIMARY KEY NOT NULL,
    award_name VARCHAR(100)
);

CREATE TABLE player_awards(
    player_id INT UNSIGNED NOT NULL,
    award_id TINYINT UNSIGNED NOT NULL,
    season_id VARCHAR(10) NOT NULL,
    month VARCHAR(10),
    week DATE,
    PRIMARY KEY (player_id, award_id, season_id),
    FOREIGN KEY (player_id) REFERENCES player_info(player_id),
    FOREIGN KEY (award_id) REFERENCES awards(award_id)
);

CREATE TABLE player_scoreboards(
    player_id INT UNSIGNED NOT NULL,
    game_id int UNSIGNED NOT NULL,
    game_date DATE,
    matchup VARCHAR(20),
    winlose CHAR(1),
    pts TINYINT UNSIGNED,
    fgm TINYINT UNSIGNED,
    fga TINYINT UNSIGNED,
    fg_pct DECIMAL(4,3),
    fg3m TINYINT UNSIGNED,
    fg3a TINYINT UNSIGNED,
    fg3_pct DECIMAL(4,3),
    ftm TINYINT UNSIGNED,
    fta TINYINT UNSIGNED,
    ft_pct DECIMAL(4,3),
    oreb TINYINT UNSIGNED,
    dreb TINYINT UNSIGNED,
    reb TINYINT UNSIGNED,
    ast TINYINT UNSIGNED,
    tov TINYINT UNSIGNED,
    stl TINYINT UNSIGNED,
    blk TINYINT UNSIGNED,
    pf TINYINT UNSIGNED,
    plus_minus TINYINT,
    minutes VARCHAR(10),
    speed FLOAT,
    distance FLOAT,
    reb_chances_off SMALLINT UNSIGNED,
    reb_chances_def SMALLINT UNSIGNED,
    reb_chances_total SMALLINT UNSIGNED,
    touches SMALLINT UNSIGNED,
    secondary_ast SMALLINT,
    ft_ast SMALLINT,
    passes SMALLINT UNSIGNED,
    fgm_contested SMALLINT,
    fga_contested SMALLINT,
    fg_pct_contested FLOAT,
    fgm_uncontested SMALLINT,
    fga_uncontested SMALLINT,
    fg_pct_uncontested FLOAT,
    fgm_defended_at_rim SMALLINT,
    fga_defended_at_rim SMALLINT,
    fg_pct_defended_at_rim FLOAT,
    est_off_rating FLOAT,
    off_rating FLOAT,
    est_def_rating FLOAT,
    def_rating FLOAT,
    est_net_rating FLOAT,
    net_rating FLOAT,
    ast_pct DECIMAL(4, 3),
    ast_to_tov FLOAT,
    ast_ratio FLOAT,
    oreb_pct DECIMAL(4, 3),
    dreb_pct DECIMAL(4, 3),
    reb_pct DECIMAL(4, 3),
    tov_ratio FLOAT,
    effective_fg_pct DECIMAL(4, 3),
    ts_pct DECIMAL(4, 3),
    est_pace FLOAT,
    pace FLOAT,
    pace_per_40 FLOAT,
    possessions TINYINT UNSIGNED,
    pie FLOAT,
    PRIMARY KEY (player_id, game_id),
    FOREIGN KEY (player_id) REFERENCES player_info(player_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

-----------------------------------------------------------------------------------------------------------------------------

---------------------
---               ---
---  TEAM_TABLES  ---
---               ---
---------------------

CREATE TABLE team_seasons(
    team_id INT UNSIGNED NOT NULL,
    season_id VARCHAR(10) NOT NULL,
    gp TINYINT UNSIGNED,
    w TINYINT UNSIGNED,
    l TINYINT UNSIGNED,
    win_pct DECIMAL(4,3),
    min SMALLINT UNSIGNED,
    fgm SMALLINT UNSIGNED,
    fga SMALLINT UNSIGNED,
    fg_pct DECIMAL(4,3),
    fg3m SMALLINT UNSIGNED,
    fg3a SMALLINT UNSIGNED,
    fg3_pct DECIMAL(4,3),
    ftm SMALLINT UNSIGNED,
    fta SMALLINT UNSIGNED,
    ft_pct DECIMAL(4,3),
    oreb SMALLINT UNSIGNED,
    dreb SMALLINT UNSIGNED,
    reb SMALLINT UNSIGNED,
    ast SMALLINT UNSIGNED,
    tov SMALLINT UNSIGNED,
    stl SMALLINT UNSIGNED,
    blk SMALLINT UNSIGNED,
    blka SMALLINT UNSIGNED,
    pf SMALLINT UNSIGNED,
    pfd SMALLINT UNSIGNED,
    pts SMALLINT UNSIGNED,
    plus_minus SMALLINT,
    PRIMARY KEY (team_id, season_id),
    FOREIGN KEY (team_id) REFERENCES team_info(team_id)    
);

CREATE TABLE team_scoreboards(
    team_id INT UNSIGNED NOT NULL,
    game_id int UNSIGNED NOT NULL,
    game_date DATE,
    matchup VARCHAR(20),
    winlose CHAR(1),
    minutes VARCHAR(10),
    pts TINYINT UNSIGNED,
    fgm TINYINT UNSIGNED,
    fga TINYINT UNSIGNED,
    fg_pct DECIMAL(4,3),
    fg3m TINYINT UNSIGNED,
    fg3a TINYINT UNSIGNED,
    fg3_pct DECIMAL(4,3),
    ftm TINYINT UNSIGNED,
    fta TINYINT UNSIGNED,
    ft_pct DECIMAL(4,3),
    oreb TINYINT UNSIGNED,
    dreb TINYINT UNSIGNED,
    reb TINYINT UNSIGNED,
    ast TINYINT UNSIGNED,
    tov TINYINT UNSIGNED,
    stl TINYINT UNSIGNED,
    blk TINYINT UNSIGNED,
    blka TINYINT UNSIGNED,
    pf TINYINT UNSIGNED,
    plus_minus TINYINT,
    distance FLOAT,
    reb_chances_off SMALLINT UNSIGNED,
    reb_chances_def SMALLINT UNSIGNED,
    reb_chances_total SMALLINT UNSIGNED,
    touches INT UNSIGNED,
    secondary_ast SMALLINT,
    ft_ast SMALLINT,
    passes INT UNSIGNED,
    fgm_contested SMALLINT,
    fga_contested SMALLINT,
    fg_pct_contested FLOAT,
    fgm_uncontested SMALLINT,
    fga_uncontested SMALLINT,
    fg_pct_uncontested FLOAT,
    fgm_defended_at_rim SMALLINT,
    fga_defended_at_rim SMALLINT,
    fg_pct_defended_at_rim FLOAT,
    est_off_rating FLOAT,
    off_rating FLOAT,
    est_def_rating FLOAT,
    def_rating FLOAT,
    est_net_rating FLOAT,
    net_rating FLOAT,
    ast_pct DECIMAL(4, 3),
    ast_to_tov FLOAT,
    ast_ratio FLOAT,
    oreb_pct DECIMAL(4, 3),
    dreb_pct DECIMAL(4, 3),
    reb_pct DECIMAL(4, 3),
    est_tov_pct FLOAT,
    tov_ratio FLOAT,
    effective_fg_pct DECIMAL(4, 3),
    ts_pct DECIMAL(4, 3),
    est_pace FLOAT,
    pace FLOAT,
    pace_per_40 FLOAT,
    possessions TINYINT UNSIGNED,
    PRIMARY KEY (team_id, game_id),
    FOREIGN KEY (team_id) REFERENCES team_info(team_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

CREATE TABLE lineups(
    group_id INT UNSIGNED PRIMARY KEY NOT NULL,
    group_name VARCHAR(250),
    player_1 INT UNSIGNED NOT NULL,
    player_2 INT UNSIGNED NOT NULL,
    player_3 INT UNSIGNED NOT NULL,
    player_4 INT UNSIGNED NOT NULL,
    player_5 INT UNSIGNED NOT NULL,
    team_id INT UNSIGNED NOT NULL,
    gp SMALLINT UNSIGNED,
    w SMALLINT UNSIGNED,
    l SMALLINT UNSIGNED,
    win_pct DECIMAL(4, 3),
    minutes FLOAT(2),
    fgm SMALLINT UNSIGNED,
    fga SMALLINT UNSIGNED,
    fg_pct DECIMAL(4, 3),
    fg3m SMALLINT UNSIGNED,
    fg3a SMALLINT UNSIGNED,
    fg3_pct DECIMAL(4, 3),
    ftm SMALLINT UNSIGNED,
    fta SMALLINT UNSIGNED,
    ft_pct DECIMAL(4, 3),
    oreb SMALLINT UNSIGNED,
    dreb SMALLINT UNSIGNED,
    reb SMALLINT UNSIGNED,
    ast SMALLINT UNSIGNED,
    tov SMALLINT UNSIGNED,
    stl SMALLINT UNSIGNED,
    blk SMALLINT UNSIGNED,
    blka SMALLINT UNSIGNED,
    pf SMALLINT UNSIGNED,
    pfd SMALLINT UNSIGNED,
    pts SMALLINT UNSIGNED,
    plus_minus SMALLINT,
    off_rating FLOAT,
    def_rating FLOAT,
    net_rating FLOAT,
    pace FLOAT,
    ts_pct DECIMAL(4, 3),
    fta_rate FLOAT,
    team_ast_pct DECIMAL(4, 3),
    pct_fga_2pt DECIMAL(4, 3),
    pct_fga_3pt DECIMAL(4, 3),
    pct_pts_mr DECIMAL(4, 3),
    pct_pts_fb DECIMAL(4, 3),
    pct_pts_ft DECIMAL(4, 3),
    pct_pts_paint DECIMAL(4, 3),
    pct_ast_fgm DECIMAL(4, 3),
    pct_uast_fgm DECIMAL(4, 3),
    opp_fg3_pct DECIMAL(4, 3),
    opp_efg_pct DECIMAL(4, 3),
    opp_fta_rate FLOAT,
    opp_tov_pct TINYINT UNSIGNED,
    FOREIGN KEY (player_1) REFERENCES player_info(player_id),
	FOREIGN KEY (player_2) REFERENCES player_info(player_id),
	FOREIGN KEY (player_3) REFERENCES player_info(player_id),
	FOREIGN KEY (player_4) REFERENCES player_info(player_id),
	FOREIGN KEY (player_5) REFERENCES player_info(player_id),
    FOREIGN KEY (team_id) REFERENCES team_info(team_id)
);