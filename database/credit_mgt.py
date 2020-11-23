from sqlalchemy import Table
from sqlalchemy.sql import select
from flask_sqlalchemy import SQLAlchemy
from config import engine

db = SQLAlchemy()


class CreditCard(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    card_num = db.Column(db.String(4), primary_key=True)


Credit_tbl = Table('credit_card', CreditCard.metadata)


def create_credit_table():
    CreditCard.metadata.create_all(engine)


def add_credit(id, card_num):

    user_cards = get_user_cards(id)
    user_cards = [r[0] for r in user_cards]

    if card_num not in user_cards:
        ins = Credit_tbl.insert().values(id=id, card_num=card_num,)
        conn = engine.connect()
        conn.execute(ins)
        conn.close()


def del_user_cards(id):
    delete = Credit_tbl.delete().where(Credit_tbl.c.username == id)

    conn = engine.connect()
    conn.execute(delete)
    conn.close()


def show_all_credit():
    select_st = select([Credit_tbl.c.id, Credit_tbl.c.card_num])

    conn = engine.connect()
    rs = conn.execute(select_st)

    for row in rs:
        print(row)

    conn.close()


def get_user_cards(id):
    stmt = select([Credit_tbl.c.card_num]).where(Credit_tbl.c.id == id)
    conn = engine.connect()
    rs = conn.execute(stmt)

    conn.close()
    return rs


def del_all_cards():
    delete = Credit_tbl.delete()

    conn = engine.connect()
    conn.execute(delete)
    conn.close()