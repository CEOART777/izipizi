-- Users seed for future DB integration.
-- Column names are kept as provided by the project request.

CREATE TABLE IF NOT EXISTS users_data (
    ID_user INT PRIMARY KEY,
    ID_curs INT NULL,
    ID__homework INT NULL,
    reward_coins INT NOT NULL,
    email VARCHAR(200) NOT NULL,
    phone VARCHAR(30) NOT NULL,
    role VARCHAR(50) NOT NULL,
    full_name VARCHAR(200) NOT NULL,
    balanse_coins INT NULL
);

INSERT INTO users_data (ID_user, ID_curs, ID__homework, reward_coins, email, phone, role, full_name, balanse_coins) VALUES
    (1001, 101, 501, 50, 'ivanov@mail.ru', '+79161234567', 'student', 'Иван Иванов', NULL),
    (1002, 101, 502, 100, 'petrova@mail.ru', '+79169876543', 'student', 'Анна Петрова', NULL),
    (1003, 103, 503, 200, 'sidorov@mail.ru', '+79155554433', 'student', 'Петр Сидоров', NULL),
    (1004, NULL, NULL, 25, 'admin@platform.ru', '+74951234567', 'admin', 'Админ Админов', NULL),
    (1005, 102, 504, 100, 'teacher@platform.ru', '+79167778899', 'teacher', 'Мария Учителева', NULL),
    (1006, 104, 505, 200, 'kozlova@mail.ru', '+79163334455', 'student', 'Елена Козлова', NULL);
