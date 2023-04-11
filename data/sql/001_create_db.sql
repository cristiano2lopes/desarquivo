CREATE TABLE source_dim (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE location_dim (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE category_dim (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE date_dim (
    id INTEGER PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    UNIQUE (year, month, day)
);

CREATE TABLE fact (
    id TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    accessory_content TEXT,
    source_url TEXT NOT NULL,
    arquivo_url TEXT NOT NULL,
    screenshot_url TEXT,
    canonical_url TEXT NOT NULL,
    inserted_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at DATETIME,
    version TEXT,
    date_id INTEGER NOT NULL,
    category_id INTEGER NOT NULL,
    source_id INTEGER NOT NULL,
    location_id INTEGER,
    FOREIGN KEY (date_id) REFERENCES date_dim(id),
    FOREIGN KEY (category_id) REFERENCES category_dim(id),
    FOREIGN KEY (source_id) REFERENCES source_dim(id),
    FOREIGN KEY (location_id) REFERENCES location_dim(id)
);