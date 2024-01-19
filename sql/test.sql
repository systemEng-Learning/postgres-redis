CREATE TABLE test (
    id serial8 NOT NULL PRIMARY KEY,
    title varchar(50),
    description text,
    payload jsonb
);
INSERT INTO test (title, description, payload) VALUES ('Fox', 'a description', '{"key": "value"}');
INSERT INTO test (title, description, payload) VALUES ('Hox', 'a mispelling', '{"select": "update"}');