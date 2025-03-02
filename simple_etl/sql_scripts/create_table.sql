CREATE TABLE IF NOT EXISTS
    tmp_user_table(id INTEGER PRIMARY KEY AUTOINCREMENT,
                   user_id TEXT,
                   first_name TEXT,
                   last_name TEXT,
                   age TEXT,
                   gender TEXT,
                   email TEXT,
                   eye_color TEXT,
                   latitude REAL,
                   longitude REAL,
                   country TEXT,
                   most_bought_category TEXT
                   );