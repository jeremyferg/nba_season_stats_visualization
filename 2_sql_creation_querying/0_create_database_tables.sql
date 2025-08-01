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

-----------------------
---                 ---
---  PLAYER_TABLES  ---
---                 ---
-----------------------

CREATE TABLE player_info(
    player_id INT UNSIGNED PRIMARY KEY NOT NULL,
    first_name VARCHAR(30) NOT NULL,
    last_name VARCHAR(30) NOT NULL,
    birthdate DATE NOT NULL,
    school VARCHAR(50) NOT NULL,
    country VARCHAR(30) NOT NULL,
    height VARCHAR(4) NOT NULL,
    weight SMALLINT UNSIGNED NOT NULL,
    season_exp TINYINT UNSIGNED NOT NULL,
    jersey VARCHAR(2) NOT NULL,
    position VARCHAR(20) NOT NULL,
    from_year SMALLINT UNSIGNED NOT NULL,
    to_year SMALLINT UNSIGNED NOT NULL,
    gleague CHAR(1) NOT NULL,
    draft_year SMALLINT UNSIGNED NOT NULL,
    drat_round VARCHAR(10) NOT NULL,
    draft_num VARCHAR(10) NOT NULL,
    nba_75 CHAR(1) NOT NULL
);

CREATE TABLE player_career(
    player_id INT UNSIGNED NOT NULL,
    team_id INT UNSIGNED NOT NULL,
    gp SMALLINT UNSIGNED NOT NULL,
    gs SMALLINT UNSIGNED NOT NULL,
    num_years TINYINT UNSIGNED NOT NULL,
    min INT UNSIGNED NOT NULL,
    fgm SMALLINT UNSIGNED NOT NULL,
    fga SMALLINT UNSIGNED NOT NULL,
    fg_pct DECIMAL(4,3) NOT NULL,
    fg3m SMALLINT UNSIGNED NOT NULL,
    fg3a SMALLINT UNSIGNED NOT NULL,
    fg3_pct DECIMAL(4,3) NOT NULL,
    ftm SMALLINT UNSIGNED NOT NULL,
    fta SMALLINT UNSIGNED NOT NULL,
    ft_pct DECIMAL(4,3) NOT NULL,
    oreb SMALLINT UNSIGNED NOT NULL,
    dreb SMALLINT UNSIGNED NOT NULL,
    reb SMALLINT UNSIGNED NOT NULL,
    ast SMALLINT UNSIGNED NOT NULL,
    stl SMALLINT UNSIGNED NOT NULL,
    blk SMALLINT UNSIGNED NOT NULL,
    tov SMALLINT UNSIGNED NOT NULL,
    pf SMALLINT UNSIGNED NOT NULL,
    pts SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (player_id, team_id),
    FOREIGN KEY player_id REFERENCES player_info(player_id),
    FOREIGN KEY team_id REFERENCES team_info(team_id)
);

CREATE TABLE player_seasons(
    player_id INT UNSIGNED NOT NULL,
    season_id VARCHAR(10) NOT NULL,
    team_id INT UNSIGNED NOT NULL,
    gp SMALLINT UNSIGNED NOT NULL,
    gs SMALLINT UNSIGNED NOT NULL,
    min INT UNSIGNED NOT NULL,
    fgm SMALLINT UNSIGNED NOT NULL,
    fga SMALLINT UNSIGNED NOT NULL,
    fg_pct DECIMAL(4,3) NOT NULL,
    fg3m SMALLINT UNSIGNED NOT NULL,
    fg3a SMALLINT UNSIGNED NOT NULL,
    fg3_pct DECIMAL(4,3) NOT NULL,
    ftm SMALLINT UNSIGNED NOT NULL,
    fta SMALLINT UNSIGNED NOT NULL,
    ft_pct DECIMAL(4,3) NOT NULL,
    oreb SMALLINT UNSIGNED NOT NULL,
    dreb SMALLINT UNSIGNED NOT NULL,
    reb SMALLINT UNSIGNED NOT NULL,
    ast SMALLINT UNSIGNED NOT NULL,
    stl SMALLINT UNSIGNED NOT NULL,
    blk SMALLINT UNSIGNED NOT NULL,
    tov SMALLINT UNSIGNED NOT NULL,
    pf SMALLINT UNSIGNED NOT NULL,
    pts SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (player_id, season_id, team_id),
    FOREIGN KEY player_id REFERENCES player_info(player_id),
    FOREIGN KEY team_id REFERENCES team_info(team_id)
);

CREATE TABLE awards(
    award_id TINYINT UNSIGNED PRIMARY KEY NOT NULL,
    award_name VARCHAR(100) NOT NULL
);

CREATE TABLE player_awards(
    player_id INT UNSIGNED NOT NULL,
    award_id TINYINT UNSIGNED NOT NULL,
    season_id VARCHAR(10) NOT NULL,
    month VARCHAR(10),
    week DATE,
    PRIMARY KEY (player_id, award_id, season_id),
    FOREIGN KEY player_id REFERENCES player_info(player_id),
    FOREIGN KEY award_id REFERENCES awards(award_id)
);

CREATE TABLE player_scoreboards(
    player_id INT UNSIGNED NOT NULL,
    team_abr VARCHAR(3) NOT NULL,
    game_id int UNSIGNED NOT NULL,
    game_date DATE NOT NULL,
    matchup VARCHAR(20) NOT NULL,
    winlose CHAR(1) NOT NULL,
    pts TINYINT UNSIGNED NOT NULL,
    fgm TINYINT UNSIGNED NOT NULL,
    fga TINYINT UNSIGNED NOT NULL,
    fg_pct DECIMAL(4,3) NOT NULL,
    fg3m TINYINT UNSIGNED NOT NULL,
    fg3a TINYINT UNSIGNED NOT NULL,
    fg3_pct DECIMAL(4,3) NOT NULL,
    ftm TINYINT UNSIGNED NOT NULL,
    fta TINYINT UNSIGNED NOT NULL,
    ft_pct DECIMAL(4,3) NOT NULL,
    oreb TINYINT UNSIGNED NOT NULL,
    dreb TINYINT UNSIGNED NOT NULL,
    reb TINYINT UNSIGNED NOT NULL,
    ast TINYINT UNSIGNED NOT NULL,
    tov TINYINT UNSIGNED NOT NULL,
    stl TINYINT UNSIGNED NOT NULL,
    blk TINYINT UNSIGNED NOT NULL,
    pf TINYINT UNSIGNED NOT NULL,
    plus_minus TINYINT NOT NULL,
    min SMALLINT UNSIGNED NOT NULL,
    speed FLOAT NOT NULL,
    distance FLOAT NOT NULL,
    reb_chances_off TINYINT UNSIGNED NOT NULL,
    reb_chances_def TINYINT UNSIGNED NOT NULL,
    reb_chances_total TINYINT UNSIGNED NOT NULL,
    touches SMALLINT UNSIGNED NOT NULL
    secondary_ast TINYINT NOT NULL,
    ft_ast TINYINT NOT NULL,
    passes SMALLINT UNSIGNED NOT NULL,
    fgm_contested TINYINT NOT NULL,
    fga_contested TINYINT NOT NULL,
    fg_pct_contested DECIMAL(4,3) NOT NULL,
    fgm_uncontested TINYINT NOT NULL,
    fga_uncontested TINYINT NOT NULL,
    fg_pct_uncontested DECIMAL(4,3) NOT NULL,
    fgm_defended_at_rim TINYINT NOT NULL,
    fga_defended_at_rim TINYINT NOT NULL,
    fg_pct_defended_at_rim DECIMAL(4,3) NOT NULL,
    est_off_rating FLOAT NOT NULL,
    off_rating FLOAT NOT NULL,
    est_def_rating FLOAT NOT NULL,
    def_rating FLOAT NOT NULL,
    est_net_rating FLOAT NOT NULL,
    net_rating FLOAT NOT NULL,
    ast_pct DECIMAL(4, 3) NOT NULL,
    ast_to_tov FLOAT NOT NULL,
    ast_ratio FLOAT NOT NULL,
    oreb_pct DECIMAL(4, 3) NOT NULL,
    dreb_pct DECIMAL(4, 3) NOT NULL,
    reb_pct DECIMAL(4, 3) NOT NULL,
    tov_ratio FLOAT NOT NULL,
    effective_fg_pct DECIMAL(4, 3) NOT NULL,
    ts_pct DECIMAL(4, 3) NOT NULL,
    est_pace FLOAT NOT NULL,
    pace FLOAT NOT NULL,
    pace_per_40 FLOAT NOT NULL,
    possessions TINYINT UNSIGNED NOT NULL
    pie DECIMAL(4, 3) NOT NULL,
    PRIMARY KEY (player_id, game_id),
    FOREIGN KEY player_id REFERENCES player_info(team_id)
    FOREIGN KEY game_id REFERENCES games(game_id)
);

-----------------------------------------------------------------------------------------------------------------------------

---------------------
---               ---
---  TEAM_TABLES  ---
---               ---
---------------------

CREATE TABLE team_info(
    team_id INT UNSIGNED PRIMARY KEY NOT NULL,
    team_city VARCHAR(20),
    team_name VARCHAR(20) NOT NULL,
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

CREATE TABLE team_seasons(
    team_id INT UNSIGNED NOT NULL,
    team_name VARCHAR(50) NOT NULL,
    season VARCHAR(10) NOT NULL,
    gp TINYINT UNSIGNED NOT NULL,
    w TINYINT UNSIGNED NOT NULL,
    l TINYINT UNSIGNED NOT NULL,
    win_pct DECIMAL(4,3) NOT NULL,
    min SMALLINT UNSIGNED NOT NULL,
    fgm SMALLINT UNSIGNED NOT NULL,
    fga SMALLINT UNSIGNED NOT NULL,
    fg_pct DECIMAL(4,3) NOT NULL,
    fg3m SMALLINT UNSIGNED NOT NULL,
    fg3a SMALLINT UNSIGNED NOT NULL,
    fg3_pct DECIMAL(4,3) NOT NULL,
    ftm SMALLINT UNSIGNED NOT NULL,
    fta SMALLINT UNSIGNED NOT NULL,
    ft_pct DECIMAL(4,3) NOT NULL,
    oreb SMALLINT UNSIGNED NOT NULL,
    dreb SMALLINT UNSIGNED NOT NULL,
    reb SMALLINT UNSIGNED NOT NULL,
    ast SMALLINT UNSIGNED NOT NULL,
    tov SMALLINT UNSIGNED NOT NULL,
    stl SMALLINT UNSIGNED NOT NULL,
    blk SMALLINT UNSIGNED NOT NULL,
    blka SMALLINT UNSIGNED NOT NULL,
    pf SMALLINT UNSIGNED NOT NULL,
    pfd SMALLINT UNSIGNED NOT NULL,
    pts SMALLINT UNSIGNED NOT NULL,
    plus_minus SMALLINT NOT NULL,
    PRIMARY KEY (team_id, season)
    FOREIGN KEY (team_id) REFERENCES teams_info(team_id)    
);

CREATE TABLE team_scoreboards(
    team_id INT UNSIGNED NOT NULL,
    game_id int UNSIGNED NOT NULL,
    game_date DATE NOT NULL,
    matchup VARCHAR(20) NOT NULL,
    winlose CHAR(1) NOT NULL,
    minutes SMALLINT UNSIGNED NOT NULL,
    pts TINYINT UNSIGNED NOT NULL,
    fgm TINYINT UNSIGNED NOT NULL,
    fga TINYINT UNSIGNED NOT NULL,
    fg_pct DECIMAL(4,3) NOT NULL,
    fg3m TINYINT UNSIGNED NOT NULL,
    fg3a TINYINT UNSIGNED NOT NULL,
    fg3_pct DECIMAL(4,3) NOT NULL,
    ftm TINYINT UNSIGNED NOT NULL,
    fta TINYINT UNSIGNED NOT NULL,
    ft_pct DECIMAL(4,3) NOT NULL,
    oreb TINYINT UNSIGNED NOT NULL,
    dreb TINYINT UNSIGNED NOT NULL,
    reb TINYINT UNSIGNED NOT NULL,
    ast TINYINT UNSIGNED NOT NULL,
    tov TINYINT UNSIGNED NOT NULL,
    stl TINYINT UNSIGNED NOT NULL,
    blk TINYINT UNSIGNED NOT NULL,
    blka TINYINT UNSIGNED NOT NULL,
    pf TINYINT UNSIGNED NOT NULL,
    plus_minus TINYINT NOT NULL,
    distance FLOAT NOT NULL,
    reb_chances_off TINYINT UNSIGNED NOT NULL,
    reb_chances_def TINYINT UNSIGNED NOT NULL,
    reb_chances_total TINYINT UNSIGNED NOT NULL,
    touches SMALLINT UNSIGNED NOT NULL
    secondary_ast TINYINT NOT NULL,
    ft_ast TINYINT NOT NULL,
    passes SMALLINT UNSIGNED NOT NULL,
    fgm_contested TINYINT NOT NULL,
    fga_contested TINYINT NOT NULL,
    fg_pct_contested DECIMAL(4,3) NOT NULL,
    fgm_uncontested TINYINT NOT NULL,
    fga_uncontested TINYINT NOT NULL,
    fg_pct_uncontested DECIMAL(4,3) NOT NULL,
    fgm_defended_at_rim TINYINT NOT NULL,
    fga_defended_at_rim TINYINT NOT NULL,
    fg_pct_defended_at_rim DECIMAL(4,3) NOT NULL,
    est_off_rating FLOAT NOT NULL,
    off_rating FLOAT NOT NULL,
    est_def_rating FLOAT NOT NULL,
    def_rating FLOAT NOT NULL,
    est_net_rating FLOAT NOT NULL,
    net_rating FLOAT NOT NULL,
    ast_pct DECIMAL(4, 3) NOT NULL,
    ast_to_tov FLOAT NOT NULL,
    ast_ratio FLOAT NOT NULL,
    oreb_pct DECIMAL(4, 3) NOT NULL,
    dreb_pct DECIMAL(4, 3) NOT NULL,
    reb_pct DECIMAL(4, 3) NOT NULL,
    est_tov_pct FLOAT NOT NULL,
    tov_ratio FLOAT NOT NULL,
    effective_fg_pct DECIMAL(4, 3) NOT NULL,
    ts_pct DECIMAL(4, 3) NOT NULL,
    est_pace FLOAT NOT NULL,
    pace FLOAT NOT NULL,
    pace_per_40 FLOAT NOT NULL,
    possessions TINYINT UNSIGNED NOT NULL
    PRIMARY KEY (team_id, game_id),
    FOREIGN KEY team_id REFERENCES teams_info(team_id)
    FOREIGN KEY game_id REFERENCES games(game_id)
);

CREATE TABLE lineups(
    group_id INT UNSIGNED PRIMARY KEY NOT NULL,
    group_name VARCHAR(250) NOT NULL,
    player_1 INT UNSIGNED NOT NULL,
    player_2 INT UNSIGNED NOT NULL,
    player_3 INT UNSIGNED NOT NULL,
    player_4 INT UNSIGNED NOT NULL,
    player_5 INT UNSIGNED NOT NULL,
    team_id INT UNSIGNED NOT NULL,
    gp SMALLINT UNSIGNED NOT NULL,
    w SMALLINT UNSIGNED NOT NULL,
    l SMALLINT UNSIGNED NOT NULL,
    win_pct DECIMAL(4, 3) NOT NULL,
    min FLOAT(2) NOT NULL,
    fgm SMALLINT UNSIGNED NOT NULL,
    fga SMALLINT UNSIGNED NOT NULL,
    fg_pct DECIMAL(4, 3) NOT NULL,
    fg3m SMALLINT UNSIGNED NOT NULL,
    fg3a SMALLINT UNSIGNED NOT NULL,
    fg3_pct DECIMAL(4, 3) NOT NULL,
    ftm SMALLINT UNSIGNED NOT NULL,
    fta SMALLINT UNSIGNED NOT NULL,
    ft_pct DECIMAL(4, 3) NOT NULL,
    oreb SMALLINT UNSIGNED NOT NULL,
    dreb SMALLINT UNSIGNED NOT NULL,
    reb SMALLINT UNSIGNED NOT NULL,
    ast SMALLINT UNSIGNED NOT NULL,
    stl SMALLINT UNSIGNED NOT NULL,
    blk SMALLINT UNSIGNED NOT NULL,
    blka SMALLINT UNSIGNED NOT NULL,
    pf SMALLINT UNSIGNED NOT NULL,
    pfd SMALLINT UNSIGNED NOT NULL,
    plus_minus TINYINT NOT NULL,
    off_rating FLOAT NOT NULL,
    def_rating FLOAT NOT NULL,
    net_rating FLOAT NOT NULL,
    pace FLOAT NOT NULL,
    ts_pct DECIMAL(4, 3) NOT NULL,
    fta_rate DECIMAL(4, 3) NOT NULL,
    team_ast_pct DECIMAL(4, 3) NOT NULL,
    pct_fga_2pt DECIMAL(4, 3) NOT NULL,
    pct_fga_3pt DECIMAL(4, 3) NOT NULL,
    pct_pts_mr DECIMAL(4, 3) NOT NULL,
    pct_pts_fb DECIMAL(4, 3) NOT NULL,
    pct_pts_ft DECIMAL(4, 3) NOT NULL,
    pct_pts_paint DECIMAL(4, 3) NOT NULL,
    pct_ast_fgm DECIMAL(4, 3) NOT NULL,
    pct_uast_fgm DECIMAL(4, 3) NOT NULL,
    opp_fg3_pct DECIMAL(4, 3) NOT NULL,
    opp_efg_pct DECIMAL(4, 3) NOT NULL,
    opp_fta_rate DECIMAL(4, 3) NOT NULL,
    opp_tov_pct TINYINT UNSIGNED NOT NULL,
    FOREIGN KEY (player_1, player_2, player_3, player_4, player_5) REFERENCES player_info(player_id, player_id, player_id, player_id, player_id),
    FOREIGN KEY team_id REFERENCES team_info(team_id)
);

-----------------------------------------------------------------------------------------------------------------------------

---------------------
---               ---
---  GAME_TABLES  ---
---               ---
---------------------

CREATE TABLE games(
    game_id INT UNSIGNED PRIMARY KEY NOT NULL,
    game_date DATE NOT NULL,
    home_team_id INT UNSIGNED NOT NULL,
    visitor_team_id INT UNSIGNED NOT NULL,
    arena_id TINYINT UNSIGNED NOT NULL,
    nat_broadcaster_id INT UNSIGNED,
    home_broadcaster_id INT UNSIGNED,
    visitor_broadcaster_id INT UNSIGNED,
    home_team_pts TINYINT UNSIGNED NOT NULL,
    visitor_team_pts TINYINT UNSIGNED NOT NULL,
    outcome VARCHAR(10) NOT NULL
    FOREIGN KEY arena_id REFERENCES arenas(arena_id),
    FOREIGN KEY (nat_broadcaster_id, home_broadcaster_id, away_broadcaster_id) REFERENCES broadcasters(broadcaster_id, broadcaster_id, broadcaster_id)
);

CREATE TABLE arenas(
    arena_id TINYINT UNSIGNED PRIMARY KEY NOT NULL,
    arena_name VARCHAR(100),
    arena_city VARCHAR(100),
    arena_state VARCHAR(2)
);

CREATE TABLE broadcasters(
    broadcaster_id INT UNSIGNED PRIMARY KEY NOT NULL,
    broadcaster_name VARCHAR(100) NOT NULL
);

-----------------------------------------------------------------------------------------------------------------------------