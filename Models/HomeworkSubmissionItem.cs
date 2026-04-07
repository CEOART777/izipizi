namespace практика_2._0.Models;

public sealed class HomeworkSubmissionItem
{
    public int UserId { get; init; }
    public int LessonId { get; init; }
    public int HomeworkId { get; init; }
    public string FileName { get; init; } = string.Empty;
    /// <summary>Относительный URL в пределах wwwroot (начинается с "/").</summary>
    public string Url { get; init; } = string.Empty;
    public DateTime UploadedAt { get; init; }
}

