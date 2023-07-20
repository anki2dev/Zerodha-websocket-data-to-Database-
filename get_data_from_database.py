import databasemanager
import datetime

class DatabaseManagerApp:
    def __init__(self):
        self.db_manager = databasemanager.DatabaseManager()
        self.df_len = 0

    def run(self):
        while True:
            df = self.db_manager.get_table_data()
            # if new tick added in database then print the database
            if len(df) != self.df_len:
                print(datetime.datetime.now())
                self.df_len = len(df)
                print(df)

if __name__ == "__main__":
    app = DatabaseManagerApp()
    app.run()