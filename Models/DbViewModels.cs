namespace практика_2._0.Models;

public sealed class CourseItem
{
    public int ID_curs { get; init; }
    public int ID_lesson { get; init; }
    public int ID_categorise { get; init; }
    public decimal price { get; init; }
    public string Course_name { get; init; } = string.Empty;
    /// <summary>Рейтинг для отображения: при наличии отзывов — среднее по таблице отзывов, иначе поле rating из курса в БД.</summary>
    public decimal rating { get; init; }
    /// <summary>Число отзывов (после загрузки через <c>GetCoursesAsync</c>); 0 если отзывов нет.</summary>
    public int ReviewCount { get; init; }
    public DateTime create_at { get; init; }
    public string preview_url { get; init; } = string.Empty;
    public int? teacher_user_id { get; init; }
}

public sealed class CourseLessonItem
{
    public int ID_cours { get; init; }
    public int ID_lesson { get; init; }
    public int? number_lesson { get; init; }
    public string Cours_name { get; init; } = string.Empty;
    public string video_url { get; init; } = string.Empty;
    public string meterials_url { get; init; } = string.Empty;
    /// <summary>Текстовое описание урока (в БД часто колонка course_discription).</summary>
    public string? course_discription { get; init; }
}

public sealed class LessonProgressItem
{
    public int ID_lesson { get; init; }
    public int ID_homework { get; init; }
    public int Progres_bal { get; init; }
    public string status { get; init; } = string.Empty;
    public int? grade { get; init; }
}

public sealed class UserItem
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
    public decimal? balance_rub { get; init; }
    public bool? is_blocked { get; init; }
    public DateTime? blocked_until { get; init; }
    public string? block_reason { get; init; }
    public bool? is_deleted { get; init; }
}

public sealed class AdminStatsItem
{
    public int TotalUsers { get; init; }
    public int StudentsCount { get; init; }
    public int ActiveCoursesCount { get; init; }
    public decimal AvgCoursePrice { get; init; }
    public decimal TotalPotentialRevenue { get; init; }
    public decimal TotalPaidRevenue { get; init; }
    public int TotalEnrollments { get; init; }
    public int TotalPayments { get; init; }
    public int TotalAdViews { get; init; }
}

public sealed class EnrollmentItem
{
    public int ID_enrolment { get; init; }
    public int ID_cours { get; init; }
    public int ID_user { get; init; }
    public string PAY_method { get; init; } = string.Empty;
    public string status { get; init; } = string.Empty;
    public decimal price { get; init; }
}

public sealed class PaymentItem
{
    public int ID_user { get; init; }
    public int ID_homework { get; init; }
    public int ID_curs { get; init; }
    public int ID_enrolment { get; init; }
    public string PAY_method { get; init; } = string.Empty;
    public string status { get; init; } = string.Empty;
    public decimal price { get; init; }
}

public sealed class HomeworkItem
{
    public int ID_lesson { get; init; }
    public int ID_homework { get; init; }
    public int Progres_bal { get; init; }
    public string status { get; init; } = string.Empty;
    public int? grade { get; init; }
}

public sealed class AdItem
{
    public int ID_AD { get; init; }
    public string AD_type { get; init; } = string.Empty;
    public int reward_coins { get; init; }
}

public sealed class ShowAdItem
{
    public int ID_AD { get; init; }
    public int ID_user { get; init; }
    public int reward_coins { get; init; }
}

public sealed class CourseReviewItem
{
    public int ID_review { get; init; }
    public int ID_cours { get; init; }
    public int ID_user { get; init; }
    public decimal rating { get; init; }
    public string comment_text { get; init; } = string.Empty;
    public DateTime created_at { get; init; }
    public string user_name { get; init; } = string.Empty;
}
