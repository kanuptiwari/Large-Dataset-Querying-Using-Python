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
