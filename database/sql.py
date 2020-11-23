import mysql.connector
from mysql.connector.cursor import MySQLCursorDict


class MySqlConnection:

    def __init__(self, DB_HOST, DB_USER, DB_PASSWORD, DB_NAME):
        self.host = DB_HOST
        self.name = DB_NAME
        self.user = DB_USER
        self.password = DB_PASSWORD
        self.conn = None

    def get_conn(self):
        self.conn = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.name
        )
        return self.conn


mydbconnobj = MySqlConnection('localhost', 'root', 'root', 'mydatabase')
mydbconn = mydbconnobj.get_conn()


def insert_transaction(transaction):
    mycursor = mydbconn.cursor()

    sql = "INSERT INTO transaction (date, business, sector,amount, credit_card ) VALUES (%s, %s, %s, %s ,%s )"
    mycursor.execute(sql, transaction)

    mydbconn.commit()


def getRecordsByMonth(year, month):
    mycursor = mydbconn.cursor(cursor_class=MySQLCursorDict)

    # mycursor.execute("SELECT sector, SUM(amount) FROM transaction GROUP BY sector")

    # mycursor.execute("SELECT * FROM transaction WHERE MONTH(date) = 1")

    mycursor.execute("SELECT * FROM transaction  WHERE MONTH(date) = %s AND YEAR(date) = %s", (month, year))

    records = mycursor.fetchall()
    colums = mycursor.column_names

    return records, colums


def getAmountGroupBySector():
    mycursor = mydbconn.cursor(cursor_class=MySQLCursorDict)
    mycursor.execute("SELECT sector, SUM(amount) FROM transaction GROUP BY sector")

    records = mycursor.fetchall()
    return records
