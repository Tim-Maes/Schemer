CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(120) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(12,2) NOT NULL,
    description TEXT,
    category_id INTEGER,
    is_active BOOLEAN DEFAULT 1
);

CREATE TABLE categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL,
    description TEXT
);

INSERT INTO users (name, email, phone) VALUES ('John Doe', 'john@example.com', '555-1234');
INSERT INTO categories (name, description) VALUES ('Electronics', 'Electronic devices');
INSERT INTO products (name, price, description, category_id, is_active) VALUES ('Widget A', 19.99, 'A useful widget', 1, 1);