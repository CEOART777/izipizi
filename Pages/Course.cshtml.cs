using System.ComponentModel.DataAnnotations;
using System.Security.Claims;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

public class CourseModel : PageModel
{
    private const string LastCourseCookie = "last_course_id";
    private const string LastLessonCookie = "last_lesson_id";
    private readonly AccessDbService _accessDbService;
    private readonly ILogger<CourseModel> _logger;

    public CourseModel(AccessDbService accessDbService, ILogger<CourseModel> logger)
    {
        _accessDbService = accessDbService;
        _logger = logger;
    }

    public CourseItem? Course { get; private set; }
    public IReadOnlyList<CourseLessonItem> Lessons { get; private set; } = [];
    public IReadOnlyList<CourseReviewItem> Reviews { get; private set; } = [];
    /// <summary>Средний рейтинг по отзывам; если отзывов нет — берётся rating из таблицы курса.</summary>
    public decimal DisplayAverageRating { get; private set; }
    /// <summary>Текст описания курса из поля course_discription урока (первое непустое по урокам курса).</summary>
    public string? CourseDescription { get; private set; }
    public bool CanLeaveReview { get; private set; }
    public bool ShowPayPrompt { get; private set; }
    public bool IsEnrolled { get; private set; }
    /// <summary>Запись ENROLLMENTS со статусом finished — курс пройден, доступ к урокам для просмотра сохраняется.</summary>
    public bool IsEnrollmentFinished { get; private set; }
    public bool IsRefundRequested { get; private set; }
    public string CategoryDisplayName { get; private set; } = string.Empty;
    public string ActiveTab { get; private set; } = "description";
    public string? ReviewMessage { get; private set; }

    [BindProperty]
    public ReviewInput Input { get; set; } = new();

    public async Task OnGetAsync(int? courseId, string? tab, CancellationToken cancellationToken)
    {
        var id = await ResolveCourseIdAsync(courseId, cancellationToken);
        ActiveTab = NormalizeTab(tab);

        try
        {
            Course = await _accessDbService.GetCourseByIdAsync(id, cancellationToken);
            Lessons = await _accessDbService.GetLessonsByCourseIdAsync(id, cancellationToken);
            try
            {
                Reviews = await _accessDbService.GetReviewsByCourseIdAsync(id, cancellationToken);
            }
            catch (Exception reviewsEx)
            {
                _logger.LogWarning(reviewsEx, "Reviews are temporarily unavailable.");
                Reviews = [];
            }
            await LoadReviewPermissionAsync(id, cancellationToken);
            await LoadEnrollmentPermissionAsync(id, cancellationToken);
            await LoadCategoryDisplayNameAsync(cancellationToken);
            RefreshDerivedCourseFields();
            if (Course is not null)
            {
                SaveLastViewedCourse(Course.ID_curs, Lessons.FirstOrDefault()?.ID_lesson);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load course page data.");
            Course = null;
            Lessons = [];
            Reviews = [];
            DisplayAverageRating = 0;
            CourseDescription = null;
            CanLeaveReview = false;
            ShowPayPrompt = false;
            IsEnrolled = false;
            IsEnrollmentFinished = false;
            CategoryDisplayName = string.Empty;
        }
    }

    public async Task<IActionResult> OnPostAddReviewAsync(int courseId, CancellationToken cancellationToken)
    {
        ActiveTab = "reviews";
        await LoadCourseDataAsync(courseId, cancellationToken);
        if (Course is null)
        {
            return RedirectToPage("/Index");
        }

        if (!ModelState.IsValid)
        {
            ReviewMessage = "Проверьте оценку и текст отзыва.";
            return Page();
        }

        var userId = await ResolveCurrentUserIdAsync(cancellationToken);
        if (userId is null)
        {
            return RedirectToPage("/Login", new { returnUrl = $"/Course?courseId={courseId}&tab=reviews" });
        }

        if (User.IsInRole("admin") || User.IsInRole("teacher"))
        {
            CanLeaveReview = true;
            ShowPayPrompt = false;
        }
        else
        {
            CanLeaveReview = await _accessDbService.HasUserPaidCourseAsync(userId.Value, courseId, cancellationToken);
            ShowPayPrompt = !CanLeaveReview;
        }

        if (!CanLeaveReview)
        {
            ReviewMessage = "Оставить отзыв можно только после оплаты курса.";
            return Page();
        }

        var normalizedRating = Math.Clamp(Input.Rating, 1, 5);
        var normalizedText = (Input.Comment ?? string.Empty).Trim();
        var saved = await _accessDbService.UpsertCourseReviewAsync(
            userId.Value,
            courseId,
            normalizedRating,
            normalizedText,
            cancellationToken);

        if (!saved)
        {
            ReviewMessage = "Не удалось сохранить отзыв. Попробуйте снова.";
            return Page();
        }

        return RedirectToPage("/Course", new { courseId, tab = "reviews" });
    }

    /// <summary>
    /// 1) Явный courseId в URL. 2) Cookie последнего просмотренного курса.
    /// 3) Первый курс из каталога БД (по ID_curs), без фиксированного 101.
    /// </summary>
    private async Task<int> ResolveCourseIdAsync(int? routeCourseId, CancellationToken cancellationToken)
    {
        if (routeCourseId.HasValue && routeCourseId.Value > 0)
        {
            return routeCourseId.Value;
        }

        if (int.TryParse(HttpContext.Request.Cookies[LastCourseCookie], out var cookieCourseId) && cookieCourseId > 0)
        {
            return cookieCourseId;
        }

        var courses = await _accessDbService.GetCoursesAsync(cancellationToken);
        var first = courses.OrderBy(c => c.ID_curs).FirstOrDefault();
        return first?.ID_curs ?? 0;
    }

    private void SaveLastViewedCourse(int courseId, int? lessonId)
    {
        var options = new CookieOptions
        {
            Expires = DateTimeOffset.UtcNow.AddDays(30),
            IsEssential = true
        };

        HttpContext.Response.Cookies.Append(LastCourseCookie, courseId.ToString(), options);
        if (lessonId.HasValue && lessonId.Value > 0)
        {
            HttpContext.Response.Cookies.Append(LastLessonCookie, lessonId.Value.ToString(), options);
        }
    }

    private async Task LoadCourseDataAsync(int courseId, CancellationToken cancellationToken)
    {
        Course = await _accessDbService.GetCourseByIdAsync(courseId, cancellationToken);
        Lessons = await _accessDbService.GetLessonsByCourseIdAsync(courseId, cancellationToken);
        try
        {
            Reviews = await _accessDbService.GetReviewsByCourseIdAsync(courseId, cancellationToken);
        }
        catch (Exception reviewsEx)
        {
            _logger.LogWarning(reviewsEx, "Reviews are temporarily unavailable.");
            Reviews = [];
        }
        await LoadReviewPermissionAsync(courseId, cancellationToken);
        await LoadCategoryDisplayNameAsync(cancellationToken);
        RefreshDerivedCourseFields();
        if (Course is not null)
        {
            SaveLastViewedCourse(Course.ID_curs, Lessons.FirstOrDefault()?.ID_lesson);
        }
    }

    private void RefreshDerivedCourseFields()
    {
        // rating в Course уже среднее по отзывам из БД (см. AccessDbService.GetCoursesAsync) либо поле курса, если отзывов нет.
        DisplayAverageRating = Course?.rating ?? 0;

        CourseDescription = Lessons
            .Select(l => l.course_discription)
            .FirstOrDefault(static s => !string.IsNullOrWhiteSpace(s));
    }

    private async Task LoadReviewPermissionAsync(int courseId, CancellationToken cancellationToken)
    {
        if (User.IsInRole("admin") || User.IsInRole("teacher"))
        {
            CanLeaveReview = true;
            ShowPayPrompt = false;
            return;
        }

        var userId = await ResolveCurrentUserIdAsync(cancellationToken);
        if (userId is null)
        {
            CanLeaveReview = false;
            ShowPayPrompt = User.Identity?.IsAuthenticated == true;
            return;
        }

        CanLeaveReview = await _accessDbService.HasUserPaidCourseAsync(userId.Value, courseId, cancellationToken);
        ShowPayPrompt = !CanLeaveReview;
    }

    private async Task LoadEnrollmentPermissionAsync(int courseId, CancellationToken cancellationToken)
    {
        if (User.IsInRole("admin") || User.IsInRole("teacher"))
        {
            IsEnrolled = true;
            IsRefundRequested = false;
            return;
        }

        var userId = await ResolveCurrentUserIdAsync(cancellationToken);
        if (userId is null)
        {
            IsEnrolled = false;
            return;
        }

        IsEnrolled = await _accessDbService.IsUserEnrolledInCourseAsync(userId.Value, courseId, cancellationToken);
        IsEnrollmentFinished = false;
        IsRefundRequested = false;
        if (IsEnrolled)
        {
            var enr = await _accessDbService.GetEnrollmentByUserAndCourseAsync(userId.Value, courseId, cancellationToken);
            IsEnrollmentFinished = string.Equals((enr?.status ?? string.Empty).Trim(), "finished", StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            var enr = await _accessDbService.GetEnrollmentByUserAndCourseAsync(userId.Value, courseId, cancellationToken);
            IsRefundRequested = string.Equals((enr?.status ?? string.Empty).Trim(), "refund_requested", StringComparison.OrdinalIgnoreCase);
        }
    }

    private async Task LoadCategoryDisplayNameAsync(CancellationToken cancellationToken)
    {
        if (Course is null)
        {
            CategoryDisplayName = string.Empty;
            return;
        }

        var categories = await _accessDbService.GetCategoriesAsync(cancellationToken);
        CategoryDisplayName = categories.FirstOrDefault(c => c.ID_categorise == Course.ID_categorise)?.name_categori
                              ?? $"Категория #{Course.ID_categorise}";
    }

    private async Task<int?> ResolveCurrentUserIdAsync(CancellationToken cancellationToken)
    {
        var email = HttpContext.User.FindFirstValue(ClaimTypes.Email);
        if (!string.IsNullOrWhiteSpace(email))
        {
            var userByEmail = await _accessDbService.GetUserByEmailAsync(email.Trim().Trim('"'), cancellationToken);
            if (userByEmail is not null)
            {
                return userByEmail.ID_user;
            }
        }

        var claimValue = HttpContext.User.FindFirstValue(ClaimTypes.NameIdentifier);
        if (int.TryParse(claimValue, out var claimUserId) && claimUserId > 0)
        {
            var byId = await _accessDbService.GetUserByIdAsync(claimUserId, cancellationToken);
            if (byId is not null)
            {
                return claimUserId;
            }
        }

        return null;
    }

    private static string NormalizeTab(string? tab)
    {
        if (string.Equals(tab, "program", StringComparison.OrdinalIgnoreCase))
        {
            return "program";
        }

        if (string.Equals(tab, "reviews", StringComparison.OrdinalIgnoreCase))
        {
            return "reviews";
        }

        return "description";
    }

    public sealed class ReviewInput
    {
        [Range(1, 5, ErrorMessage = "Оценка должна быть от 1 до 5.")]
        public decimal Rating { get; set; } = 5;

        [Required(ErrorMessage = "Введите отзыв.")]
        [StringLength(2000, ErrorMessage = "Слишком длинный отзыв.")]
        public string Comment { get; set; } = string.Empty;
    }
}
