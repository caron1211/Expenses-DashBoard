from csv import reader
import os


def insetCsvToRedis(redis_conn, file_name):
    # skip first line i.e. read header first and then iterate over each row od csv as a list
    with open(os.path.join(os.path.dirname(__file__), file_name), 'r') as read_obj:
        csv_reader = reader(read_obj)
        header = next(csv_reader)
        # Check file as empty
        if header is not None:
            # Iterate over each row after the header in the csv
            for row in csv_reader:
                # row variable is a list that represents a row in csv
                key = row[0]
                value = row[1]
                redis_conn.set(key, value)  # set in the database

