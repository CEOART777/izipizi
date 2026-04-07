-- Categories seed for future DB integration.
-- Column names are kept as provided by the project request.

CREATE TABLE IF NOT EXISTS categories (
    ID_categorise INT PRIMARY KEY,
    name_categori VARCHAR(120) NOT NULL,
    discription VARCHAR(500) NOT NULL
);

INSERT INTO categories (ID_categorise, name_categori, discription) VALUES
    (1, 'Програмирование', 'Курсы по разработке на Python, JaavaScript и других'),
    (2, 'Дизайн', 'UI/UX дизайн, графический дизайн, Figma'),
    (3, 'Маркетинг', 'SMM, таргет, контекстная реклама'),
    (4, 'Аналитика', 'Data Science, Excel, SQL'),
    (5, 'Иностранные языки', 'Английский, немецкий, китайский');
