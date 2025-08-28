CREATE TABLE IF NOT EXISTS astros_data (
    id SERIAL PRIMARY KEY,
    craft VARCHAR(100),
    name VARCHAR(255),
    _inserted_at TIMESTAMP default timezone('UTC', CURRENT_TIMESTAMP),
    UNIQUE(craft, name) 
);