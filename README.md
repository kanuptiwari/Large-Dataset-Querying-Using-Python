# Large-Dataset-Querying-Using-Python
# requirements for the project are:
#cloudpickle==1.2.2
#dask==2.5.2
#fsspec==0.5.2
#locket==0.2.0
mock==3.0.5
#numexpr==2.7.0
#numpy==1.17.2
#pandas==0.25.1
#partd==1.0.0
#PySide2==5.13.1
#python-dateutil==2.8.0
#pytz==2019.3
#shiboken2==5.13.1
#six==1.12.0
#tables==3.5.2
#toolz==0.10.0

# User Interface source code
import sys

from PySide2.QtCore import Qt
from PySide2.QtGui import QIntValidator
from PySide2.QtWidgets import QApplication, QLabel, QDialog, QVBoxLayout, QHBoxLayout, QComboBox, QLineEdit, \
    QPushButton, QGroupBox, QTableView, QHeaderView, QCheckBox

from model import PandasModel
from query import QueryHelper


class MainWindow(QDialog):
    __width = 1024
    __height = 768

    __control_margin = 5
    __control_height = 25

    def __init__(self, parent=None):
        super(MainWindow, self).__init__(parent)
        self.setWindowTitle("Python Pandas Dask Demo")
        self.setFixedSize(MainWindow.__width, MainWindow.__height)

        # Primary layout for the window.
        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(main_layout)

        # Window header.
        header_layout = QHBoxLayout()
        header_layout.setContentsMargins(20, 0, 20, 0)
        header = QLabel("Please select the dataset and the query to run.")
        header.setFixedHeight(50)
        header.setStyleSheet("font-size: 16px; border-bottom:1px; border-style: solid;")
        header_layout.addWidget(header)
        main_layout.addLayout(header_layout)

        # First control row
        dataset_holder = QHBoxLayout()
        dataset_holder.setContentsMargins(20, 10, 20, 0)

        dataset_label = QLabel("Dataset under query : ")
        dataset_label.setFixedWidth(130)
        dataset_label.setStyleSheet(f"margin-right: {MainWindow.__control_margin}px")
        dataset_holder.addWidget(dataset_label)

        self.dataset_combo = QComboBox()
        self.dataset_combo.setFixedWidth(250)
        self.dataset_combo.setFixedHeight(MainWindow.__control_height)
        self.dataset_combo.currentIndexChanged.connect(self.dataset_combo_change)
        dataset_holder.addWidget(self.dataset_combo)

        self.dataset_desc_label = QLabel()
        self.dataset_desc_label.setStyleSheet("color: blue; font-weight: bold;")
        dataset_holder.addWidget(self.dataset_desc_label, stretch=1)

        main_layout.addLayout(dataset_holder)

        # Second control row.
        query_holder = QHBoxLayout()
        query_holder.setContentsMargins(20, 5, 20, 0)

        query_label = QLabel("Query : ")
        query_label.setFixedWidth(130)
        query_label.setStyleSheet(f"margin-right: {MainWindow.__control_margin}px")
        query_holder.addWidget(query_label)

        self.query_combo = QComboBox()
        self.query_combo.setFixedWidth(400)
        self.query_combo.setFixedHeight(MainWindow.__control_height)
        query_holder.addWidget(self.query_combo)

        slice_label = QLabel("Slice : ")
        slice_label.setStyleSheet(f"margin-right: {MainWindow.__control_margin}px")
        query_holder.addWidget(slice_label)

        self.slice_combo = QComboBox()
        self.slice_combo.setFixedWidth(80)
        self.slice_combo.setFixedHeight(MainWindow.__control_height)
        query_holder.addWidget(self.slice_combo)

        self.slice_input = QLineEdit()
        self.slice_input.setFixedWidth(80)
        self.slice_input.setFixedHeight(MainWindow.__control_height)
        self.slice_input.setAlignment(Qt.AlignRight)
        self.slice_input.setValidator(QIntValidator(0, 10000, self))
        self.slice_input.setStyleSheet(f"margin-right: {MainWindow.__control_margin}px;")
        query_holder.addWidget(self.slice_input)

        self.parallel_processing = QCheckBox("Parallel processing")
        query_holder.addWidget(self.parallel_processing)

        self.run_button = QPushButton("Run Query")
        self.run_button.clicked.connect(self.run_query)
        query_holder.addWidget(self.run_button, stretch=1)

        main_layout.addLayout(query_holder)

        group_box = QGroupBox("Query result")
        group_box.setStyleSheet(
            "QGroupBox { margin: 20px; margin-top: 0px; }"
            "QGroupBox::title { subcontrol-origin: margin; left: 40px; top: 10px; padding: 0px 5px 0px 5px; }")
        main_layout.addWidget(group_box, stretch=1)

        group_box_layout = QVBoxLayout()
        group_box_layout.setContentsMargins(10, 30, 10, 10)

        # Data table widget.
        self.data_table = QTableView()
        self.data_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        group_box_layout.addWidget(self.data_table)
        group_box.setLayout(group_box_layout)

        # Add data to the fixed controls.
        self.init_data()

        # Disable the query controls.
        self.toggle_query(False)

    def init_data(self):
        self.dataset_combo.insertItem(0, "Please select a dataset")
        for dataset in QueryHelper.DataSets:
            self.dataset_combo.insertItem(int(dataset['id']), dataset['name'])

        self.slice_combo.insertItem(0, 'Head', 'head')
        self.slice_combo.insertItem(0, 'Tail', 'tail')

    def dataset_combo_change(self):
        selected = self.dataset_combo.currentIndex()
        if selected <= 0:
            self.query_combo.clear()
            self.query_combo.insertItem(0, "Please select a dataset")
            self.toggle_query(False)
        else:
            self.query_combo.clear()
            dataset = list(filter(lambda d: (d['id'] == selected), QueryHelper.DataSets))[0]
            self.dataset_desc_label.setText(dataset['description'])
            for query in dataset['queries']:
                self.query_combo.insertItem(query['id'], query['name'])

            self.toggle_query(True)

    def toggle_query(self, enabled):
        self.query_combo.setEnabled(enabled)
        self.slice_combo.setEnabled(enabled)
        if not enabled:
            self.slice_input.setText('0')
            self.parallel_processing.setCheckState(Qt.Unchecked)
        self.slice_input.setEnabled(enabled)
        self.parallel_processing.setEnabled(enabled)
        self.run_button.setEnabled(enabled)

    def run_query(self):
        selected_dataset = self.dataset_combo.currentIndex()
        dataset = list(filter(lambda d: (d['id'] == selected_dataset), QueryHelper.DataSets))[0]
        selected_query = self.query_combo.currentIndex()
        query = list(filter(lambda q: (q['id'] == selected_query), dataset['queries']))[0]
        is_parallel = self.parallel_processing.isChecked()
        result = getattr(QueryHelper, query['func'])(is_parallel)

        # Apply slice.
        direction = self.slice_combo.currentData()
        count = int(self.slice_input.text())
        if count > 0:
            result = getattr(result, direction)(count)

        model = PandasModel(result)
        self.data_table.setModel(model)

    @staticmethod
    def show_window():
        # Create the Qt application
        app = QApplication(sys.argv)
        main_window = MainWindow()
        main_window.show()
        sys.exit(app.exec_())

# App
from ui import MainWindow


def main():
    MainWindow.show_window()


if __name__ == '__main__':
    main()

# Model
from PySide2.QtCore import QAbstractTableModel, Qt


class PandasModel(QAbstractTableModel):

    def __init__(self, data, parent=None):
        QAbstractTableModel.__init__(self, parent)
        self._data = data

    def rowCount(self, parent=None):
        return len(self._data.values)

    def columnCount(self, parent=None):
        return self._data.columns.size

    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            if role == Qt.DisplayRole:
                return str(self._data.values[index.row()][index.column()])

        return None

    def headerData(self, col, orientation, role=None):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self._data.columns[col]
        if orientation == Qt.Vertical and role == Qt.DisplayRole:
            return self._data.index[col]
        return None

# Query
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
