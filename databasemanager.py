import psycopg2
import pandas as pd
import configparser

config = configparser.ConfigParser()
config.read('config.ini')


db_username = config.get('Database', 'db_username')
db_password = config.get('Database', 'db_password')

class DatabaseManager:
    def __init__(self):
        self.conn = psycopg2.connect(
            database ="websocket_database",
            user = db_username,
            password = db_password,
            host ="localhost",
            port ="5432"
        )
        self.cursor = self.conn.cursor()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS live_stream_data (
            tradable BOOLEAN,
            mode TEXT,
            instrument_token INTEGER,
            last_price NUMERIC,
            last_traded_quantity INTEGER,
            average_traded_price NUMERIC,
            volume_traded NUMERIC,
            total_buy_quantity INTEGER,
            total_sell_quantity INTEGER,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            change NUMERIC
        );
        """
        self.cursor.execute(create_table_query)
        self.conn.commit()

    def get_table_data(self):
        table_name = 'live_stream_data'
        select_query = f"SELECT * FROM {table_name};"
        self.cursor.execute(select_query)
        rows = self.cursor.fetchall()
        column_names = [desc[0] for desc in self.cursor.description]
        df = pd.DataFrame(rows, columns=column_names)
        return df

    def add_new_column(self):
        table_name = 'live_stream_data'
        new_column_name = 'tick_date'
        new_column_type = 'TIMESTAMP'
        alter_table_query = f"ALTER TABLE {table_name} ADD COLUMN {new_column_name} {new_column_type};"
        self.cursor.execute(alter_table_query)
        self.conn.commit()

    def update_data(self, tick_date, item):
        if isinstance(item, dict):
            item = [item]
        for data in item:
            table_name = 'live_stream_data'
            insert_query = f"INSERT INTO {table_name} (tick_date, tradable, mode, instrument_token, last_price, last_traded_quantity, average_traded_price, volume_traded, total_buy_quantity, total_sell_quantity, open, high, low, close, change) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = [
                tick_date,
                data['tradable'],
                data['mode'],
                data['instrument_token'],
                data['last_price'],
                data['last_traded_quantity'],
                data['average_traded_price'],
                data['volume_traded'],
                data['total_buy_quantity'],
                data['total_sell_quantity'],
                data['ohlc']['open'],
                data['ohlc']['high'],
                data['ohlc']['low'],
                data['ohlc']['close'],
                data['change']
            ]
            self.cursor.execute(insert_query, values)
            self.conn.commit()

    def close_connection(self):
        self.cursor.close()
        self.conn.close()


# Usage Example:

# db_manager = DatabaseManager()
# db_manager.create_table()
# db_manager.add_new_column()
#data = {'tradable': True, 'mode': 'quote', 'instrument_token': 12345, 'last_price': 100.0, 'last_traded_quantity': 10, 'average_traded_price': 99.5, 'volume_traded': 1000, 'total_buy_quantity': 500, 'total_sell_quantity': 500, 'ohlc': {'open': 98.5, 'high': 101.0, 'low': 97.5, 'close': 100.0}, 'change': 0.5}
# db_manager.update_data('2023-07-15', data)
# df = db_manager.get_table_data()
# print(df)
# db_manager.close_connection()
