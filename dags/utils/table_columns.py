def get_table_columns(table_name):
    columns = {
        "hitters": [
            "hitter_id", "player_name", "team_name", "avg", "games", "pa", "ab", "runs", "hits", "doubles", "triples", "hr",
            "rbi", "sb", "cs", "sac", "sf", "bb", "ibb", "hbp", "so", "gdp", "slg", "obp", "errors", "sb_percentage", "mh", "ops", "risp", "ph_ba"
        ],
        "hitter_games": [
            "hitter_id", "game_date", "opponent_team", "avg", "pa", "ab", "runs", "hits", "doubles", "triples", "hr",
            "rbi", "sb", "cs", "bb", "hbp", "so", "gdp"
        ],
        "hitter_opponents": [
            "hitter_id", "opponent_team", "games", "avg", "pa", "ab", "runs", "hits", "doubles", "triples", "hr",
            "rbi", "sb", "cs", "bb", "hbp", "so", "gdp"
        ],
        "hitter_stadiums": [
            "hitter_id", "stadium", "games", "avg", "pa", "ab", "runs", "hits", "doubles", "triples", "hr",
            "rbi", "sb", "cs", "bb", "hbp", "so", "gdp"
        ],
        "pitchers": [
            "pitcher_id", "player_name", "team_name", "era", "games", "cg", "sho", "wins", "losses", "sv", "hld",
            "wpct", "tbf", "np", "ip", "hits", "doubles", "triples", "hr", "sac", "sf", "bb", "ibb", "so", "wp", "bk",
            "runs", "er", "bsv", "whip", "avg", "qs"
        ],
        "pitcher_games": [
            "pitcher_id", "game_date", "opponent_team", "result", "era", "tbf", "ip", "hits", "hr",
            "bb", "hbp", "so", "runs", "er", "avg"
        ],
        "pitcher_opponents": [
            "pitcher_id", "opponent_team", "games", "era", "wins", "losses", "sv", "hld", "wpct", "tbf",
            "ip", "hits", "hr", "bb", "hbp", "so", "runs", "er", "avg"
        ],
        "pitcher_stadiums": [
            "pitcher_id", "stadium", "games", "era", "wins", "losses", "sv", "hld", "wpct", "tbf", "ip",
            "hits", "hr", "bb", "hbp", "so", "runs", "er", "avg"
        ],
        "hitter_records": [
            "hitter_id", "player_name", "team_name", "game_date", "position",
            "avg", "games", "pa", "ab", "runs", "hits", "doubles", "triples", "hr", "rbi", "sb", "cs", "sac", "sf",
            "bb", "ibb", "hbp", "so", "gdp", "slg", "obp", "errors", "sb_percentage", "mh", "ops", "risp", "ph_ba",
            "wOBA", "wRC", "wRC_plus", "OPS_plus", "k_rate", "bb_rate", "BABIP",
            "opponent_team", "opponent_games", "opponent_avg", "opponent_pa", "opponent_ab", "opponent_runs",
            "opponent_hits", "opponent_doubles", "opponent_triples", "opponent_hr", "opponent_rbi", "opponent_sb",
            "opponent_cs", "opponent_bb", "opponent_hbp", "opponent_so", "opponent_gdp",
            "stadium", "stadium_games", "stadium_avg", "stadium_pa", "stadium_ab", "stadium_runs",
            "stadium_hits", "stadium_doubles", "stadium_triples", "stadium_hr", "stadium_rbi",
            "stadium_sb", "stadium_cs", "stadium_bb", "stadium_hbp", "stadium_so", "stadium_gdp",
            "recent_games_file_path"
        ],
        "pitcher_records": [
            "pitcher_id", "player_name", "team_name", "game_date",
            "era", "games", "cg", "sho", "wins", "losses", "sv", "hld", "wpct", "tbf", "np", "ip",
            "hits", "doubles", "triples", "hr", "sac", "sf", "bb", "ibb", "so", "wp", "bk", "runs", "er", "bsv", "whip", "avg", "qs",
            "FIP", "k_rate", "bb_rate", "hr_rate",
            "opponent_team", "opponent_games", "opponent_era", "opponent_wins", "opponent_losses", "opponent_sv", "opponent_hld",
            "opponent_wpct", "opponent_tbf", "opponent_ip", "opponent_hits", "opponent_hr", "opponent_bb", "opponent_hbp",
            "opponent_so", "opponent_runs", "opponent_er", "opponent_avg",
            "stadium", "stadium_games", "stadium_era", "stadium_wins", "stadium_losses", "stadium_sv", "stadium_hld",
            "stadium_wpct", "stadium_tbf", "stadium_ip", "stadium_hits", "stadium_hr", "stadium_bb", "stadium_hbp",
            "stadium_so", "stadium_runs", "stadium_er", "stadium_avg", "recent_games_file_path"
        ],
        "game_records": ["game_date", "away_team", "away_score", "home_team", "home_score", "stadium"]
    }

    return columns.get(table_name, "")