from sqlalchemy import Table, func
from sqlalchemy.sql import select
from flask_sqlalchemy import SQLAlchemy
from config import engine
import pandas as pd

db = SQLAlchemy()


class Transaction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    business = db.Column(db.String(80))
    date = db.Column(db.Date)
    sector = db.Column(db.String(80))
    amount = db.Column(db.Float)
    credit_card = db.Column(db.String(4))


Transaction_tbl = Table('transaction', Transaction.metadata)


def create_transaction_table():
    Transaction.metadata.create_all(engine)


def add_transaction(business, date, sector, amount, credit_card):
    ins = Transaction_tbl.insert().values(business=business, date=date, sector=sector, amount=amount,
                                          credit_card=credit_card)

    conn = engine.connect()
    conn.execute(ins)
    conn.close()


def del_transaction(id):
    delete = Transaction_tbl.delete().where(Transaction_tbl.c.id == id)

    conn = engine.connect()
    conn.execute(delete)
    conn.close()


def show_all_transaction():
    select_st = select([Transaction_tbl.c.business, Transaction_tbl.c.date,
                        Transaction_tbl.c.sector, Transaction_tbl.c.amount, Transaction_tbl.c.credit_card])

    conn = engine.connect()
    # rs = conn.execute(select_st)
    rs = pd.read_sql(select_st, conn)

    # for row in rs:
    #     print(row)

    conn.close()
    return rs


def get_transaction_by_card_and_date(card_num, month, year):
    select_st = select([Transaction_tbl.c.business, Transaction_tbl.c.date,
                        Transaction_tbl.c.sector, Transaction_tbl.c.amount, Transaction_tbl.c.credit_card]).where(
        (Transaction_tbl.c.credit_card == card_num) & (func.MONTH(Transaction_tbl.c.date) == month) & (func.YEAR(Transaction_tbl.c.date) == year))

    conn = engine.connect()
    # rs = conn.execute(select_st)
    rs = pd.read_sql(select_st, conn)

    conn.close()
    return rs


def get_transaction_by_date(month, year):
    select_st = select([Transaction_tbl.c.business, Transaction_tbl.c.date,
                        Transaction_tbl.c.sector, Transaction_tbl.c.amount, Transaction_tbl.c.credit_card]).where(
         (func.MONTH(Transaction_tbl.c.date) == month) & (func.YEAR(Transaction_tbl.c.date) == year))

    conn = engine.connect()
    # rs = conn.execute(select_st)
    rs = pd.read_sql(select_st, conn)
    conn.close()
    return rs


def get_transaction_by_year(year):
    select_st = select([Transaction_tbl.c.business, Transaction_tbl.c.date,
                        Transaction_tbl.c.sector, Transaction_tbl.c.amount, Transaction_tbl.c.credit_card]).where(
         func.YEAR(Transaction_tbl.c.date) == year)

    conn = engine.connect()
    # rs = conn.execute(select_st)
    rs = pd.read_sql(select_st, conn)
    conn.close()
    return rs


def del_all_transaction():
    delete = Transaction_tbl.delete()

    conn = engine.connect()
    conn.execute(delete)
    conn.close()


def get_all_sectors():
    select_st = select([Transaction_tbl.c.sector], distinct=True)
    conn = engine.connect()
    rs = pd.read_sql(select_st, conn)
    conn.close()
    return rs


if __name__ == '__main__':
    del_all_transaction()