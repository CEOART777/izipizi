-- Course lessons seed for future DB integration.
-- Column names are kept as provided by the project request.

CREATE TABLE IF NOT EXISTS course_lessons (
    ID_cours INT NOT NULL,
    ID_lesson INT PRIMARY KEY,
    Cours_name VARCHAR(200) NOT NULL,
    video_url VARCHAR(255) NOT NULL,
    meterials_url VARCHAR(255) NOT NULL
);

INSERT INTO course_lessons (ID_cours, ID_lesson, Cours_name, video_url, meterials_url) VALUES
    (101, 201, 'Введение в Python', '/videos/python_1.mp4', '/materials/python_1.pdf'),
    (101, 202, 'Переменные и типы данных', '/videos/python_2.mp4', '/materials/python_2.pdf'),
    (101, 203, 'Условные операторы', '/videos/python_3.mp4', '/materials/python_3.pdf'),
    (102, 205, 'Основы JavaScript', '/videos/js_1.mp4', '/materials/js_1.pdf'),
    (102, 206, 'Функции в JS', '/videos/js_2.mp4', '/materials/js_2.pdf'),
    (103, 210, 'Интерфейс Figma', '/videos/figma_1.mp4', '/materials/figma_1.pdf'),
    (104, 215, 'Введение в SMM', '/videos/smm_1.mp4', '/materials/smm_1.pdf'),
    (105, 220, 'Python для данных', '/videos/data_1.mp4', '/materials/data_1.pdf');
