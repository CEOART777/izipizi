namespace практика_2._0.Data;

public sealed class CourseLessonSeed
{
    public int ID_cours { get; init; }
    public int ID_lesson { get; init; }
    public string Cours_name { get; init; } = string.Empty;
    public string video_url { get; init; } = string.Empty;
    public string meterials_url { get; init; } = string.Empty;
}

public static class CourseLessonSeedData
{
    // Temporary in-code seed used before real DB wiring.
    public static readonly IReadOnlyList<CourseLessonSeed> CourseLessons =
    [
        new CourseLessonSeed
        {
            ID_cours = 101,
            ID_lesson = 201,
            Cours_name = "Введение в Python",
            video_url = "/videos/python_1.mp4",
            meterials_url = "/materials/python_1.pdf"
        },
        new CourseLessonSeed
        {
            ID_cours = 101,
            ID_lesson = 202,
            Cours_name = "Переменные и типы данных",
            video_url = "/videos/python_2.mp4",
            meterials_url = "/materials/python_2.pdf"
        },
        new CourseLessonSeed
        {
            ID_cours = 101,
            ID_lesson = 203,
            Cours_name = "Условные операторы",
            video_url = "/videos/python_3.mp4",
            meterials_url = "/materials/python_3.pdf"
        },
        new CourseLessonSeed
        {
            ID_cours = 102,
            ID_lesson = 205,
            Cours_name = "Основы JavaScript",
            video_url = "/videos/js_1.mp4",
            meterials_url = "/materials/js_1.pdf"
        },
        new CourseLessonSeed
        {
            ID_cours = 102,
            ID_lesson = 206,
            Cours_name = "Функции в JS",
            video_url = "/videos/js_2.mp4",
            meterials_url = "/materials/js_2.pdf"
        },
        new CourseLessonSeed
        {
            ID_cours = 103,
            ID_lesson = 210,
            Cours_name = "Интерфейс Figma",
            video_url = "/videos/figma_1.mp4",
            meterials_url = "/materials/figma_1.pdf"
        },
        new CourseLessonSeed
        {
            ID_cours = 104,
            ID_lesson = 215,
            Cours_name = "Введение в SMM",
            video_url = "/videos/smm_1.mp4",
            meterials_url = "/materials/smm_1.pdf"
        },
        new CourseLessonSeed
        {
            ID_cours = 105,
            ID_lesson = 220,
            Cours_name = "Python для данных",
            video_url = "/videos/data_1.mp4",
            meterials_url = "/materials/data_1.pdf"
        }
    ];
}
