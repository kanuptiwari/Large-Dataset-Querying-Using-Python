from time import clock

import pandas as pd
from dask import dataframe as dd


class QueryHelper:
    """ This class holds the functions used for querying the data sets and the control data structure."""

    # Control data structure.
    DataSets = [{
        'id': 1,
        'name': 'European Soccer Database',
        'description': '25k+ matches, players & teams attributes for European Professional Football',
        'queries': [{
            'id': 0,
            'name': 'Select all teams playing for an English league',
            'func': 'get_teams_in_eng_league'
        }, {
            'id': 1,
            'name': 'Number of matches by league',
            'func': 'no_of_matches_by_league'
        }],
        'src': 'https://www.kaggle.com/hugomathien/soccer'
    }, {
        'id': 2,
        'name': 'FIFA 19 Player Database',
        'description': '18k+ FIFA 19 players, ~90 attributes extracted from the latest FIFA database',
        'queries': [{
            'id': 0,
            'name': 'All the players who are English nationals',
            'func': 'eng_players'
        }, {
            'id': 1,
            'name': 'Top performing football club - 2019',
            'func': 'top_performing_club'
        }],
        'src': 'https://www.kaggle.com/karangadiya/fifa19'
    }, {
        'id': 3,
        'name': 'FIFA 19 / European Soccer Combined',
        'description': 'Combined dataset of European Soccer Database and FIFA 19 Player Database',
        'queries': [{
            'id': 0,
            'name': 'Not Implemented',
            'func': 'eng_players'
        }],
        'src': 'Please refer to the individual data sets'
    }]

    H5StorePath = './data/data_store.h5'

    @staticmethod
    def get_teams_in_eng_league(is_parallel=False):
        """ Get teams playing in the English leagues"""

        # Start the timer.
        start_time = clock()

        # Read the CSV files.
        countries = QueryHelper.read_data('./data/soccer/Country.csv')
        leagues = QueryHelper.read_data('./data/soccer/League.csv')
        teams = QueryHelper.read_data('./data/soccer/Team.csv',
                                      cols=['id', 'team_api_id', 'team_long_name', 'team_short_name'])
        matches = QueryHelper.read_data('./data/soccer/Match.csv', cols=['id', 'country_id', 'home_team_api_id'])

        # Merge country and league and rename columns.
        country_league = countries.merge(leagues, left_on='id', right_on='id', how='outer').drop('id', axis=1).rename(
            columns={'name_x': 'country', 'name_y': 'league'})

        # Find the leagues where the country is 'England'.
        eng_league = country_league.loc[country_league['country'] == 'England']

        # Merge the English leagues with matches, rename columns and drop the additional 'id' column.
        eng_league_matches = eng_league.merge(matches, left_on='country_id', right_on='country_id', how='inner').drop(
            'id', axis=1).rename(columns={'home_team_api_id': 'team_api_id'})

        # Merge the previous result with teams and drop duplicates to get the final result.
        teams_in_eng_league = eng_league_matches.merge(teams, left_on='team_api_id', right_on='team_api_id',
                                                       how='inner').drop('id', axis=1).drop('country_id',
                                                                                            axis=1).drop_duplicates(
            subset=None, keep='first', inplace=False)

        # Compute the result and store it in the file system.
        h5_store_key = 'teams_in_eng_league'
        QueryHelper.store(h5_store_key, teams_in_eng_league.nsmallest(len(teams_in_eng_league.index), 'team_api_id'),
                          is_parallel)

        result = QueryHelper.retrieve(h5_store_key).rename(
            columns={'country': 'Country', 'league': 'League', 'team_api_id': 'Team API ID',
                     'team_long_name': 'Team Name', 'team_short_name': 'ABR'})[
            ['Team API ID', 'Country', 'League', 'ABR', 'Team Name']]

        exec_time = clock() - start_time
        print(f'Execution time : {exec_time} seconds')

        return result

    @staticmethod
    def no_of_matches_by_league(is_parallel=False):
        """ Get the count of matches by league """

        # Start the timer.
        start_time = clock()

        # Read the CSV files.
        leagues = QueryHelper.read_data('./data/soccer/League.csv')
        matches = QueryHelper.read_data('./data/soccer/Match.csv', cols=['id', 'country_id', 'home_team_api_id'])

        # Merge the league matches and filter only the required columns.
        league_matches = matches.merge(leagues, left_on='country_id', right_on='country_id', how='inner')[['name']]
        league_matches['matches'] = 1
        league_matches = league_matches.groupby(['name']).count().reset_index(drop=False)
        league_matches['ID'] = league_matches.index + 1

        # Compute the result and store it in the file system.
        h5_store_key = 'league_matches'
        QueryHelper.store(h5_store_key, league_matches, is_parallel)

        result = QueryHelper.retrieve(h5_store_key).rename(columns={'name': 'Name', 'matches': 'Matches'})[
            ['ID', 'Name', 'Matches']]

        exec_time = clock() - start_time
        print(f'Execution time : {exec_time} seconds')

        return result

    @staticmethod
    def eng_players(is_parallel=False):
        """ Get all the players who are English nationals """

        # Start the timer.
        start_time = clock()

        # Read the CSV file.
        df = QueryHelper.read_data('./data/fifa/FIFA.csv',
                                   cols=['ID', 'Name', 'Age', 'Nationality', 'Club', 'PreferredFoot', 'BodyType'])

        # Select players whose nationality is English.
        eng_players = df.loc[df['Nationality'] == 'England']
        # Reset the index and drop the ID column.
        eng_players = eng_players.reset_index(drop=True).drop('ID', axis=1)

        # Add an ID column.
        eng_players['ID'] = eng_players.index + 1

        # Compute the result and store it in the file system.
        h5_store_key = 'eng_players'
        QueryHelper.store(h5_store_key, eng_players, is_parallel)

        result = QueryHelper.retrieve(h5_store_key).rename(
            columns={'PreferredFoot': 'Preferred Foot', 'BodyType': 'Body Type'})[
            ['ID', 'Name', 'Nationality', 'Club', 'Preferred Foot', 'Body Type']]

        exec_time = clock() - start_time
        print(f'Execution time : {exec_time} seconds')

        return result

    @staticmethod
    def top_performing_club(is_parallel=False):
        """ Get top performing clubs - 2019"""
        pd.set_option('display.max_columns', None)

        # Start the timer.
        start_time = clock()

        # Read the CSV file.
        df = QueryHelper.read_data('./data/fifa/FIFA.csv', cols=['Overall', 'Potential', 'Club'])
        club_avg_performance = df.groupby(['Club']).mean()

        # Sort the dataset by overall performance ascending order.
        club_avg_performance = club_avg_performance.nlargest(len(club_avg_performance.index), 'Overall').reset_index(
            drop=False)
        # Add a new column based on the index.
        club_avg_performance['ID'] = club_avg_performance.index + 1

        # Compute the result and store it in the file system.
        h5_store_key = 'club_avg_performance'
        QueryHelper.store(h5_store_key, club_avg_performance, is_parallel)

        result = QueryHelper.retrieve(h5_store_key)[['ID', 'Club', 'Overall', 'Potential']].round(2)

        exec_time = clock() - start_time
        print(f'Execution time : {exec_time} seconds')

        return result

    @staticmethod
    def read_data(csv_path, cols=None):
        if cols is None:
            dataframe = dd.read_csv(csv_path, delimiter=',', blocksize=64000000)
        else:
            dataframe = dd.read_csv(csv_path, delimiter=',', usecols=cols, blocksize=64000000)

        return dataframe

    @staticmethod
    def store(key, dataframe, is_parallel=False):
        """ Compute and store the results in the H5 data store. """

        store = pd.HDFStore(QueryHelper.H5StorePath)
        if is_parallel:
            store.put(key, dataframe.compute(scheduler='processes'), format='table', data_columns=True)
        else:
            store.put(key, dataframe.compute(), format='table', data_columns=True)

    @staticmethod
    def retrieve(key):
        """ Retrieve data from the H5 data store. """

        store = pd.HDFStore(QueryHelper.H5StorePath)
        return store.get(key)
