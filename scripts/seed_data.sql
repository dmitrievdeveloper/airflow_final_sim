
-- Создание raw_table
CREATE TABLE IF NOT EXISTS raw_table (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50),
    amount NUMERIC
);

INSERT INTO raw_table (category, amount) VALUES 
('A', 100),
('B', 200),
('A', 150),
('C', 50),
('B', 100);

-- Создание raw_table_2
CREATE TABLE IF NOT EXISTS raw_table_2 (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50),
    amount NUMERIC
);

INSERT INTO raw_table_2 (category, amount) VALUES 
('X', 1000),
('Y', 2000),
('X', 500),
('Z', 300);
