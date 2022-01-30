from gbq import GBQ
g = GBQ()

g.get_players_team(2021)

g.get_three_pointer(2021)

g.get_avg_pts(2021)

g.get_game_result('DEN')

g.get_player_pts(2021, 'DEN')

g.get_player_metric(2021, 'DEN', 'SUM', 'pts')

