-- Lesson progress seed for future DB integration.
-- Column names are kept as provided by the project request.

CREATE TABLE IF NOT EXISTS lesson_progress (
    ID_lesson INT NOT NULL,
    ID_homework INT PRIMARY KEY,
    Progres_bal INT NOT NULL,
    status VARCHAR(50) NOT NULL,
    grade INT NULL
);

INSERT INTO lesson_progress (ID_lesson, ID_homework, Progres_bal, status, grade) VALUES
    (201, 501, 85, 'completed', 4),
    (201, 502, 92, 'completed', 5),
    (202, 503, 70, 'completed', 3),
    (202, 504, 88, 'completed', 4),
    (203, 505, 95, 'completed', 5),
    (210, 506, 100, 'completed', 5),
    (215, 507, 45, 'pending', NULL),
    (220, 508, 60, 'cheking', NULL);
