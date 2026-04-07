-- Courses seed for future DB integration.
-- Column names are kept as provided by the project request.

CREATE TABLE IF NOT EXISTS courses (
    ID_curs INT PRIMARY KEY,
    ID_lesson INT NOT NULL,
    ID_categorise INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    Course_name VARCHAR(200) NOT NULL,
    rating DECIMAL(3,2) NOT NULL,
    create_at DATE NOT NULL,
    preview_url VARCHAR(255) NOT NULL,
    CONSTRAINT fk_courses_category FOREIGN KEY (ID_categorise)
        REFERENCES categories (ID_categorise)
);

INSERT INTO courses (ID_curs, ID_lesson, ID_categorise, price, Course_name, rating, create_at, preview_url) VALUES
    (101, 201, 1, 1500, 'Python-разработчик с нуля', 4.8, '2024-01-15', '/previews/python_course.jpg'),
    (102, 205, 1, 1200, 'JavaScript для начинающих', 4.6, '2024-02-10', 'previews/js_course.jpg'),
    (103, 210, 2, 1800, 'Figma PRO: Интерфейсы', 4.1, '2024-01-20', '/previews/figma_course.jpg'),
    (104, 215, 3, 1000, 'SMM-специалист 2025', 4.2, '2024-03-05', '/previews/smm_course.jpg'),
    (105, 220, 4, 2000, 'Data Scientist', 4.8, '2024-02-28', 'previews/data_science.jpg');
