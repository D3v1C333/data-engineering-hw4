import pickle
import sqlite3
import json


def load_data(file_name):
    with open(file_name, 'rb') as file:
        data = pickle.load(file)
    return data


def connect_to_db():
    connection = sqlite3.connect("1.db")
    connection.row_factory = sqlite3.Row
    return connection


def insert_comment_data(db, data):
    cursor = db.cursor()
    cursor.executemany(
        "INSERT INTO comment (book_id, price, place, date) VALUES((SELECT id FROM books WHERE title = :title), :price, :place, :date)",
        data)
    db.commit()


def first_query(db, title):
    cursor = db.cursor()
    result_data = []
    res = cursor.execute("SELECT * FROM comment WHERE book_id = (SELECT id FROM books WHERE title = ?)", [title])
    for row in res.fetchall():
        result_data.append(dict(row))
    with open('./output/2_first_query.json', 'w', encoding='utf-8') as json_file:
        json.dump(result_data, json_file, ensure_ascii=False, indent=2)


def second_query(db, title):
    cursor = db.cursor()
    result_data = []
    res = cursor.execute(
        "SELECT AVG(price) as avg_price FROM comment WHERE book_id = (SELECT id FROM books WHERE title = ?)", [title])
    for row in res.fetchall():
        result_data.append(dict(row))
    with open('./output/2_second_query.json', 'w', encoding='utf-8') as json_file:
        json.dump(result_data, json_file, ensure_ascii=False, indent=2)


def third_query(db):
    cursor = db.cursor()
    result_data = []
    res = cursor.execute(
        "SELECT title, (SELECT COUNT(*) FROM comment WHERE id = book_id) as count FROM books ORDER BY count DESC LIMIT 10")
    for row in res.fetchall():
        result_data.append(dict(row))
    with open('./output/2_third_query.json', 'w', encoding='utf-8') as json_file:
        json.dump(result_data, json_file, ensure_ascii=False, indent=2)


db = connect_to_db()
items = load_data('./input/2/task_2_var_11_subitem.pkl')
# insert_comment_data(db, items)
first_query(db, 'Преступление и наказание')
second_query(db, 'Солярис')
third_query(db)
