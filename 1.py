import csv
import json
import sqlite3

def parse_date(file_name):
    items = []
    with open(file_name, 'r', encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        for row in csv_reader:
            row['pages'] = int(row['pages'])
            row['published_year'] = int(row['published_year'])
            row['rating'] = float(row['rating'])
            row['views'] = int(row['views'])
            items.append(dict(row))
        return items


def connect_to_db():
    connection = sqlite3.connect("1.db")
    connection.row_factory = sqlite3.Row
    return connection


def insert_data(db, data):
    cursor = db.cursor()
    cursor.executemany(
        "INSERT INTO books (title, author, genre, pages, published_year, isbn, rating, views) VALUES(:title, :author, :genre, :pages, :published_year, :isbn, :rating, :views)",
        data)
    db.commit()


def get_top_by_views(db, limit):
    cursor = db.cursor()
    result = []
    get_top_by_views = cursor.execute(
        "SELECT * FROM books ORDER BY views  DESC LIMIT ?", [limit])
    for row in get_top_by_views.fetchall():
        result.append(dict(row))
    with open('./output/1_first_filtered_result.json', 'w', encoding='utf-8') as json_file:
        json.dump(result, json_file, ensure_ascii=False, indent=2)

    cursor.close()

def get_filtered_by_year(db, min_year, limit):
    cursor = db.cursor()
    result = []
    filtered_by_year_stat = cursor.execute(
        "SELECT * FROM books WHERE published_year > ? ORDER BY published_year LIMIT ?", [min_year, limit])
    for row in filtered_by_year_stat.fetchall():
        result.append(dict(row))
    with open('./output/1_second_filtered_result.json', 'w', encoding='utf-8') as json_file:
        json.dump(result, json_file, ensure_ascii=False, indent=2)

def get_stats(db):
    cursor = db.cursor()
    result_data = {}
    freq_res = []

    freq_by_centuary = cursor.execute(
        "SELECT CAST(count(*) as REAL) / (SELECT COUNT(*) FROM books) as count, (FLOOR(published_year/100)+ 1) as centuary FROM books GROUP BY(FLOOR(published_year/100)+1)")
    for row in freq_by_centuary.fetchall():
        freq_res.append(dict(row))
    result_data['freq_by_centuary'] = freq_res

    pages_stat = cursor.execute(
        "SELECT SUM(pages) as sum, AVG(pages) as avg, MIN(pages) as min, MAX(pages) as max FROM books")
    result_data['pages_stat'] = dict(pages_stat.fetchone())

    with open('./output/1_stats.json', 'w', encoding='utf-8') as json_file:
        json.dump(result_data, json_file, ensure_ascii=False, indent=2)

items = parse_date('./input/1/task_1_var_11_item.csv')
db = connect_to_db()
# insert_data(db, items)
get_top_by_views(db, 21)
get_stats(db)
get_filtered_by_year(db, 1857, 21)
