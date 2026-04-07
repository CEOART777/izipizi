namespace практика_2._0.Models;

public sealed class HomeworkReviewItem
{
    public int UserId { get; init; }
    public int CourseId { get; init; }
    public int LessonId { get; init; }
    public int HomeworkId { get; init; }

    /// <summary>draft | submitted | accepted | rework</summary>
    public string Status { get; init; } = string.Empty;
    public int? Grade { get; init; }
    public DateTime? SubmittedAt { get; init; }
    public DateTime? ReviewedAt { get; init; }
    public int? TeacherUserId { get; init; }
}

