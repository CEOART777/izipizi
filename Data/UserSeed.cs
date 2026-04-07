namespace практика_2._0.Data;

public sealed class UserSeed
{
    public int ID_user { get; init; }
    public int? ID_curs { get; init; }
    public int? ID__homework { get; init; }
    public int reward_coins { get; init; }
    public string email { get; init; } = string.Empty;
    public string phone { get; init; } = string.Empty;
    public string role { get; init; } = string.Empty;
    public string full_name { get; init; } = string.Empty;
    public int? balanse_coins { get; init; }
}

public static class UserSeedData
{
    // Temporary in-code seed used before real DB wiring.
    public static readonly IReadOnlyList<UserSeed> Users =
    [
        new UserSeed
        {
            ID_user = 1001,
            ID_curs = 101,
            ID__homework = 501,
            reward_coins = 50,
            email = "ivanov@mail.ru",
            phone = "+79161234567",
            role = "student",
            full_name = "Иван Иванов",
            balanse_coins = null
        },
        new UserSeed
        {
            ID_user = 1002,
            ID_curs = 101,
            ID__homework = 502,
            reward_coins = 100,
            email = "petrova@mail.ru",
            phone = "+79169876543",
            role = "student",
            full_name = "Анна Петрова",
            balanse_coins = null
        },
        new UserSeed
        {
            ID_user = 1003,
            ID_curs = 103,
            ID__homework = 503,
            reward_coins = 200,
            email = "sidorov@mail.ru",
            phone = "+79155554433",
            role = "student",
            full_name = "Петр Сидоров",
            balanse_coins = null
        },
        new UserSeed
        {
            ID_user = 1004,
            ID_curs = null,
            ID__homework = null,
            reward_coins = 25,
            email = "admin@platform.ru",
            phone = "+74951234567",
            role = "admin",
            full_name = "Админ Админов",
            balanse_coins = null
        },
        new UserSeed
        {
            ID_user = 1005,
            ID_curs = 102,
            ID__homework = 504,
            reward_coins = 100,
            email = "teacher@platform.ru",
            phone = "+79167778899",
            role = "teacher",
            full_name = "Мария Учителева",
            balanse_coins = null
        },
        new UserSeed
        {
            ID_user = 1006,
            ID_curs = 104,
            ID__homework = 505,
            reward_coins = 200,
            email = "kozlova@mail.ru",
            phone = "+79163334455",
            role = "student",
            full_name = "Елена Козлова",
            balanse_coins = null
        }
    ];
}
