CREATE TABLE news(title TEXT, source TEXT, url TEXT, image TEXT, published INT)
ALTER TABLE news MODIFY COLUMN title TEXT
    CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL;
