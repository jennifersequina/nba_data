## NBA Data using SQL 

### Description:
This project is about accessing NBA data stored in Google BigQuery using SQL syntax and Python functions.

This contains the following python files:
 - yaml_reader.py
 - gbq.py
 - main.py

### Methodology:
This section explains what I did in each python file.

1. yaml_reader.py - I created yaml_reader.py to create function for configuration, to connect with the database in Google BigQuery.
   It's important to keep the project id and credentials confidential. The yaml_reader.py will access the config.yaml I created where the project id and credentials path saved.
   

2. gbq.py - I created a class 'GBQ' with methods to connect with the database and read the data being queried and ensure it will be returned as dataframe.
Inside this class I also created the following methods where the user can input parameters based on the data the user wants to see.
   
   
   - get_players_team - this will get the teams and its players' name in the season provided.
     This one is a basic SQL syntax using SELECT, JOIN, WHERE and ORDER BY.
     
     
   - get_three_pointer - this will get the best 3-point shooter with more than 70 attempts per season provided.
     I used Common Table Expression (CTE), HAVING and ROUND for this query.
     

   - get_avg_pts - this will get the average points of teams for home and away games per season provided.
     Here I calculated first the total points per game in CTE, and used the CASE WHEN to identify if the game is at home or away, then it calculated the average points.
     

   - get_game_result - this will get results of the games per team provided.
     I used ROW NUMBER() OVER PARTITION BY to calculate the total points of the two teams per game id, and used CASE WHEN to identify which team wins or loses.
     

   - get_player_pts - this will get the players' 3-points, 2-points, and free throw per season and team provided.
     It is basic query as well using SELECT, JOIN, SUM, WHERE, GROUP BY and ORDER BY.
     

   - get_player_metric - this will get the players' 3-points, 2-points, and free throw per season, team and aggregate function provided by the user.
     Applicable aggregate function parameters here are SUM, AVG, MIN, MAX or MEDIAN, in this method I used IF to use a different SQL syntax if the aggregate function provided by user is MEDIAN or not.
     For aggregate not median, it will use the query with basic syntax while for MEDIAN, it will use the query with PERCENTILE_CONT(0.5) OVER PARTITION BY to get the median.
     

   - get_efficiency_score - this will get efficiency score of each player per team and season provided.
     I used SUM aggregate to calculate the efficiency score using the formula "NBA Efficiency Formula = (Points)+(Rebounds)+(Steals)+(Assists)+(Blocked Shots)-(Turnovers)-(Missed Shots)"
     

3. main.py - this is the main python file where the user can call the method inside the class created in gbq.py.

### Usage:
This can be useful to analyze on-field data and interpret for meaningful insights that can help on the following, but not limited to:
- assess player or team performance for improvement purposes
- come up with the best plays based on player abilities, opponent, and other factors

### For Improvement:
I started this project to further enhance my python & SQL skills using this interesting NBA dataset.
This is still subject for improvement in terms of adding more query and methods that the user can use in data analysis.
    
   