namespace практика_2._0.Data;

public sealed class CourseSeed
{
    public int ID_curs { get; init; }
    public int ID_lesson { get; init; }
    public int ID_categorise { get; init; }
    public decimal price { get; init; }
    public string Course_name { get; init; } = string.Empty;
    public decimal rating { get; init; }
    public DateOnly create_at { get; init; }
    public string preview_url { get; init; } = string.Empty;
}

public static class CourseSeedData
{
    // Temporary in-code seed used before real DB wiring.
    public static readonly IReadOnlyList<CourseSeed> Courses =
    [
        new CourseSeed
        {
            ID_curs = 101,
            ID_lesson = 201,
            ID_categorise = 1,
            price = 1500m,
            Course_name = "Python-разработчик с нуля",
            rating = 4.8m,
            create_at = new DateOnly(2024, 1, 15),
            preview_url = "/previews/python_course.jpg"
        },
        new CourseSeed
        {
            ID_curs = 102,
            ID_lesson = 205,
            ID_categorise = 1,
            price = 1200m,
            Course_name = "JavaScript для начинающих",
            rating = 4.6m,
            create_at = new DateOnly(2024, 2, 10),
            preview_url = "previews/js_course.jpg"
        },
        new CourseSeed
        {
            ID_curs = 103,
            ID_lesson = 210,
            ID_categorise = 2,
            price = 1800m,
            Course_name = "Figma PRO: Интерфейсы",
            rating = 4.1m,
            create_at = new DateOnly(2024, 1, 20),
            preview_url = "/previews/figma_course.jpg"
        },
        new CourseSeed
        {
            ID_curs = 104,
            ID_lesson = 215,
            ID_categorise = 3,
            price = 1000m,
            Course_name = "SMM-специалист 2025",
            rating = 4.2m,
            create_at = new DateOnly(2024, 3, 5),
            preview_url = "/previews/smm_course.jpg"
        },
        new CourseSeed
        {
            ID_curs = 105,
            ID_lesson = 220,
            ID_categorise = 4,
            price = 2000m,
            Course_name = "Data Scientist",
            rating = 4.8m,
            create_at = new DateOnly(2024, 2, 28),
            preview_url = "previews/data_science.jpg"
        }
    ];
}
