from google.cloud import bigquery
from google.oauth2 import service_account
from yaml_reader import read_config
import pandas as pd


class GBQ:

    def __init__(self):
        config = read_config('config/config.yaml')
        self.project_id = config['project_id']
        self.dataset = config['dataset']
        credentials = service_account.Credentials.from_service_account_file(config['credentials_path'])
        self.client = bigquery.Client(credentials=credentials, project=self.project_id)

    def _read_data(self, query_details: str) -> pd.DataFrame:
        """
        It reads the result from query and converts into dataframe
        :param query_details: SQL query
        :return: dataframe
        """
        rows = self.client.query(query=query_details).result()
        data_result = list()
        for r in rows:
            data_result.append(dict(zip(r.keys(), r.values())))
        return pd.DataFrame(data_result)

    def get_players_team(self, season: int) -> pd.DataFrame:
        """
        Get teams and player per season
        :param season: Season ID parameter like 2021, 2020 etc.
        :return: dataframe
        """

        query_details = f"""
                        SELECT DISTINCT t.team_abbreviation, t.player_name
                        FROM `{self.project_id}.{self.dataset}.traditional` t
                        JOIN `{self.project_id}.{self.dataset}.schedule` s ON s.game_id = t.game_id
                        WHERE s.season = {season}
                        ORDER BY t.team_abbreviation
                        """

        print(query_details)
        result = self._read_data(query_details)
        return result

    def get_three_pointer(self, season: int) -> pd.DataFrame:
        """
        Get the best 3-point shooter with more than 70 attempts per season
        :param season: Season ID parameter like 2021, 2020 etc.
        :return: dataframe
        """

        query_details = f"""                        
                        WITH stats AS(
                            SELECT t.player_name, SUM(t.fg3m) AS total_made, SUM(t.fg3a) AS total_attempt
                            FROM `{self.project_id}.{self.dataset}.traditional` t
                            JOIN `{self.project_id}.{self.dataset}.schedule` s ON t.game_id = s.game_id
                            WHERE s.season_type IN ('REG', 'POST') AND s.season = {season}
                            GROUP BY s.season, t.player_name
                            HAVING total_attempt > 70
                            ORDER BY total_attempt DESC
                                )
                        SELECT player_name, ROUND(total_made/total_attempt,2)*100 AS percentage
                        FROM stats
                        ORDER BY percentage DESC
                        """

        print(query_details)
        result = self._read_data(query_details)
        return result

    def get_avg_pts(self, season: int) -> pd.DataFrame:
        """
        Get the average points of teams for home and away games per season
        :param season: Season ID parameter like 2021, 2020 etc.
        :return: dataframe
        """

        query_details = f"""
                        WITH stats AS (
                          SELECT t.game_id, t.team_abbreviation, s.home, s.visitor, SUM(t.pts) AS total_pts
                          FROM `{self.project_id}.{self.dataset}.traditional` t
                          JOIN `{self.project_id}.{self.dataset}.schedule` s ON s.game_id = t.game_id
                          WHERE s.season = {season} AND s.season_type = 'REG'
                          GROUP BY t.game_id, t.team_abbreviation, s.home, s.visitor
                          ORDER BY t.game_id
                                )
                        SELECT team_abbreviation,
                         ROUND(AVG(CASE WHEN team_abbreviation = home THEN total_pts ELSE NULL END),2) AS avg_home,
                         ROUND(AVG(CASE WHEN team_abbreviation = visitor THEN total_pts ELSE NULL END),2) AS avg_away
                        FROM stats
                        GROUP BY team_abbreviation
                        """

        print(query_details)
        result = self._read_data(query_details)
        return result

    def get_game_result(self, team: str) -> pd.DataFrame:
        """
        Get results of the games per team
        :param team: Team abbreviation like DEN, BKN, etc.
        :return: dataframe
        """

        query_details = f"""
                        WITH games_table AS(
                          SELECT game_id, team_abbreviation,
                          ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY SUM(pts) DESC) AS game_num
                          FROM `{self.project_id}.{self.dataset}.traditional`
                          GROUP BY game_id, team_abbreviation
                                )
                        SELECT game_id, team_abbreviation, 
                            CASE WHEN game_num = 1 THEN 'Win' ELSE 'Lose' END AS result
                        FROM games_table
                        WHERE team_abbreviation = "{team}"
                        ORDER BY game_id
                        """

        print(query_details)
        result = self._read_data(query_details)
        return result

    def get_player_pts(self, season: int, team: str) -> pd.DataFrame:
        """
        Get the players' 3-points, 2-points, and free throw per season
        :param season: Season ID parameter like 2021, 2020 etc.
        :param team: Team abbreviation like DEN, BKN, etc.
        :return: dataframe
        """

        query_details = f"""
                        WITH scoreboard AS (
                          SELECT s.season, t.player_name,
                              SUM(IFNULL(((t.fgm-t.fg3m)*2),0)) AS two_pts,
                              SUM(IFNULL((t.fg3m*3),0)) AS three_pts,
                              SUM(IFNULL(t.ftm,0)) AS free_pts,
                              SUM(IFNULL(t.pts,0)) AS total_pts,
                          FROM `{self.project_id}.{self.dataset}.traditional` t
                          JOIN `{self.project_id}.{self.dataset}.schedule` s ON s.game_id = t.game_id
                          WHERE s.season_type = 'REG' AND s.season = {season} AND t.team_abbreviation = '{team}'
                          GROUP BY 1, 2
                          ORDER BY total_pts DESC
                        )
                        SELECT player_name, two_pts, three_pts, free_pts, total_pts FROM scoreboard
                        """

        print(query_details)
        result = self._read_data(query_details)
        return result


    def get_player_metric(self, season: int, team: str, agg: str, metric: str) -> pd.DataFrame:
        """
        Get the players' 3-points, 2-points, and free throw per season
        :param season: Season ID parameter like 2021, 2020 etc.
        :param team: Team abbreviation like DEN, BKN, etc.
        :param agg: SUM, AVG, MIN, MAX or MEDIAN
        :param metric: pts, reb, ast, stl, blk, etc.
        :return: dataframe
        """
        if agg != 'MEDIAN':
            query_details = f"""
                            WITH metrics AS (
                              SELECT s.season, t.player_name,
                                  ROUND({agg}(IFNULL(t.{metric},0)),2) AS {agg}_{metric},
                              FROM `{self.project_id}.{self.dataset}.traditional` t
                              JOIN `{self.project_id}.{self.dataset}.schedule` s ON s.game_id = t.game_id
                              WHERE s.season_type = 'REG' AND s.season = {season} AND t.team_abbreviation = '{team}'
                              GROUP BY 1, 2
                              ORDER BY {agg}_{metric} DESC
                            )
                            SELECT player_name, {agg}_{metric} FROM metrics
                            """
        else:
            query_details = f"""
                            WITH metrics AS (
                                SELECT DISTINCT s.season, t.player_name, 
                                PERCENTILE_CONT(t.{metric}, 0.5) OVER (PARTITION BY t.player_name) AS {agg}_{metric}
                                FROM `{self.project_id}.{self.dataset}.traditional` t
                                JOIN `{self.project_id}.{self.dataset}.schedule` s ON s.game_id = t.game_id
                                WHERE s.season_type = 'REG' AND s.season = {season} AND t.team_abbreviation = '{team}'
                                ORDER BY {agg}_{metric}  DESC
                                )
                            SELECT player_name, {agg}_{metric} FROM metrics
                            """

        print(query_details)
        result = self._read_data(query_details)
        return result


    def get_efficiency_score(self, season: int, team: str) -> pd.DataFrame:
        """
        Get efficiency score of each player per team per season
        :param season: Season ID parameter like 2021, 2020 etc.
        :param team: Team abbreviation like DEN, BKN, etc.
        :return: dataframe
        """
        query_details = f"""
                        WITH scoreboard AS (
                            SELECT t.player_name,
                            SUM(IFNULL(t.pts,0)) +
                            SUM(IFNULL(t.reb,0)) +
                            SUM(IFNULL(t.ast,0)) +
                            SUM(IFNULL(t.stl,0)) +
                            SUM(IFNULL(t.blk,0)) -
                            SUM(IFNULL(t.to,0)) -
                            IFNULL(t.fga - t.fgm,0) -
                            IFNULL(t.fg3a - t.fg3m,0) -
                            IFNULL(t.fta - t.ftm,0) AS score
                        FROM `{self.project_id}.{self.dataset}.traditional` t
                        JOIN `{self.project_id}.{self.dataset}.schedule` s ON s.game_id = t.game_id
                        WHERE s.season_type = 'REG' AND s.season = {season} AND t.team_abbreviation = '{team}'
                        GROUP BY 1, t.fga, t.fgm, t.fg3a, t.fg3m, t.fta, t.ftm, t.game_id
                        )
                        SELECT player_name, ROUND(AVG(score),2) AS efficiency_score FROM scoreboard
                        GROUP BY 1
                        ORDER BY efficiency_score DESC
                        """

        print(query_details)
        result = self._read_data(query_details)
        return result
