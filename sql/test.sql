CREATE TABLE test (
    id serial8 NOT NULL PRIMARY KEY,
    title varchar(50),
    description text,
    payload jsonb
);
INSERT INTO test (title, description, payload) VALUES ('Fox', 'a description', '{"key": "value"}');
INSERT INTO test (title, description, payload) VALUES ('Hox', 'a mispelling', '{"select": "update"}');

CREATE TABLE users (
    id serial8 NOT NULL PRIMARY KEY,
    first_name varchar(50),
    last_name varchar(50),
    password varchar(100)
);

INSERT INTO users (first_name, last_name, password) VALUES ('Adebayo', 'Chukwuma', '123456'), ('Usman', 'Efe', '987654'), ('Bob', 'Sydney', '987654');