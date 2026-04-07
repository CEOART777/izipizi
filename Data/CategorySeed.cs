namespace практика_2._0.Data;

public sealed class CategorySeed
{
    public int ID_categorise { get; init; }
    public string name_categori { get; init; } = string.Empty;
    public string discription { get; init; } = string.Empty;
}

public static class CategorySeedData
{
    // Temporary in-code seed used before real DB wiring.
    public static readonly IReadOnlyList<CategorySeed> Categories =
    [
        new CategorySeed
        {
            ID_categorise = 1,
            name_categori = "Програмирование",
            discription = "Курсы по разработке на Python, JaavaScript и других"
        },
        new CategorySeed
        {
            ID_categorise = 2,
            name_categori = "Дизайн",
            discription = "UI/UX дизайн, графический дизайн, Figma"
        },
        new CategorySeed
        {
            ID_categorise = 3,
            name_categori = "Маркетинг",
            discription = "SMM, таргет, контекстная реклама"
        },
        new CategorySeed
        {
            ID_categorise = 4,
            name_categori = "Аналитика",
            discription = "Data Science, Excel, SQL"
        },
        new CategorySeed
        {
            ID_categorise = 5,
            name_categori = "Иностранные языки",
            discription = "Английский, немецкий, китайский"
        }
    ];
}
