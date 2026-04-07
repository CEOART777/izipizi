namespace практика_2._0.Data;

public sealed class LessonProgressSeed
{
    public int ID_lesson { get; init; }
    public int ID_homework { get; init; }
    public int Progres_bal { get; init; }
    public string status { get; init; } = string.Empty;
    public int? grade { get; init; }
}

public static class LessonProgressSeedData
{
    // Temporary in-code seed used before real DB wiring.
    public static readonly IReadOnlyList<LessonProgressSeed> LessonProgress =
    [
        new LessonProgressSeed { ID_lesson = 201, ID_homework = 501, Progres_bal = 85, status = "completed", grade = 4 },
        new LessonProgressSeed { ID_lesson = 201, ID_homework = 502, Progres_bal = 92, status = "completed", grade = 5 },
        new LessonProgressSeed { ID_lesson = 202, ID_homework = 503, Progres_bal = 70, status = "completed", grade = 3 },
        new LessonProgressSeed { ID_lesson = 202, ID_homework = 504, Progres_bal = 88, status = "completed", grade = 4 },
        new LessonProgressSeed { ID_lesson = 203, ID_homework = 505, Progres_bal = 95, status = "completed", grade = 5 },
        new LessonProgressSeed { ID_lesson = 210, ID_homework = 506, Progres_bal = 100, status = "completed", grade = 5 },
        new LessonProgressSeed { ID_lesson = 215, ID_homework = 507, Progres_bal = 45, status = "pending", grade = null },
        new LessonProgressSeed { ID_lesson = 220, ID_homework = 508, Progres_bal = 60, status = "cheking", grade = null }
    ];
}
