using System.Security.Claims;
using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize]
public class UserProfileModel : PageModel
{
    private readonly AccessDbService _accessDbService;
    private readonly ILogger<UserProfileModel> _logger;
    private readonly IWebHostEnvironment _env;

    public UserProfileModel(AccessDbService accessDbService, ILogger<UserProfileModel> logger, IWebHostEnvironment env)
    {
        _accessDbService = accessDbService;
        _logger = logger;
        _env = env;
    }

    public UserItem? User { get; private set; }
    public IReadOnlyList<EnrollmentItem> Enrollments { get; private set; } = [];
    public IReadOnlyList<EnrollmentItem> ActiveEnrollments { get; private set; } = [];
    public IReadOnlyList<EnrollmentItem> PendingCancelEnrollments { get; private set; } = [];
    public IReadOnlyList<EnrollmentItem> FinishedEnrollments { get; private set; } = [];
    public IReadOnlyDictionary<int, CourseItem> EnrolledCourseMap { get; private set; } = new Dictionary<int, CourseItem>();
    public IReadOnlyDictionary<int, int> ActiveCourseProgressMap { get; private set; } = new Dictionary<int, int>();
    public IReadOnlyList<CourseItem> PurchasedCourses { get; private set; } = [];
    public IReadOnlyList<HomeworkSubmissionItem> HomeworkSubmissions { get; private set; } = [];
    public IReadOnlyDictionary<int, HomeworkReviewItem> HomeworkReviewMap { get; private set; } = new Dictionary<int, HomeworkReviewItem>();

    public sealed class HomeworkSubmissionViewRow
    {
        public required HomeworkSubmissionItem Submission { get; init; }
        public HomeworkReviewItem? Review { get; init; }
        /// <summary>Человекочитаемое описание урока (номер и название).</summary>
        public string LessonLabel { get; init; } = string.Empty;
    }

    public IReadOnlyList<HomeworkSubmissionViewRow> HomeworkRows { get; private set; } = [];
    [BindProperty]
    public ProfileInput Input { get; set; } = new();
    public string? SuccessMessage { get; private set; }
    public string? ErrorMessage { get; private set; }

    public sealed class ProfileInput
    {
        [Required]
        public string FullName { get; set; } = string.Empty;

        [Required]
        public string Email { get; set; } = string.Empty;

        [Required]
        public string Phone { get; set; } = string.Empty;
    }

    public async Task OnGetAsync(int? userId, CancellationToken cancellationToken)
    {
        var id = await ResolveCurrentUserIdAsync(userId, cancellationToken);
        if (id is null)
        {
            return;
        }

        try
        {
            User = await _accessDbService.GetUserByIdAsync(id.Value, cancellationToken);
            if (User is not null)
            {
                Input.FullName = User.full_name;
                Input.Email = User.email;
                Input.Phone = User.phone;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load user profile core data.");
            User = null;
        }

        try
        {
            Enrollments = await _accessDbService.GetEnrollmentsByUserIdAsync(id.Value, cancellationToken);
            var map = new Dictionary<int, CourseItem>();
            foreach (var enrollment in Enrollments)
            {
                if (map.ContainsKey(enrollment.ID_cours))
                {
                    continue;
                }

                var course = await _accessDbService.GetCourseByIdAsync(enrollment.ID_cours, cancellationToken);
                if (course is not null)
                {
                    map[enrollment.ID_cours] = course;
                }
            }
            EnrolledCourseMap = map;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load user enrollments.");
            Enrollments = [];
            EnrolledCourseMap = new Dictionary<int, CourseItem>();
        }

        // Чиним "нулевые цены" и пустой метод оплаты в ENROLLMENTS (если в базе так сохранилось ранее).
        try
        {
            await _accessDbService.BackfillEnrollmentPaymentFieldsAsync(cancellationToken);
            Enrollments = await _accessDbService.GetEnrollmentsByUserIdAsync(id.Value, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to backfill enrollment payment fields.");
        }

        // ENROLLMENTS может содержать исторические дубли (повторные оплаты/возвраты/старые записи).
        // В UI профиля показываем по одному актуальному элементу на курс: берём самую новую запись по ID_enrolment.
        static IReadOnlyList<EnrollmentItem> LatestPerCourse(IEnumerable<EnrollmentItem> items) =>
            items
                .GroupBy(e => e.ID_cours)
                .Select(g => g.OrderByDescending(x => x.ID_enrolment).First())
                .OrderByDescending(x => x.ID_enrolment)
                .ToList();

        PendingCancelEnrollments = LatestPerCourse(
            Enrollments.Where(e => string.Equals((e.status ?? string.Empty).Trim(), "refund_requested", StringComparison.OrdinalIgnoreCase)));

        ActiveEnrollments = LatestPerCourse(
            Enrollments.Where(e =>
            {
                var st = (e.status ?? string.Empty).Trim().ToLowerInvariant();
                return st != "cancelled" && st != "finished" && st != "refund_requested" && st != "complete";
            }));

        try
        {
            var progressMap = new Dictionary<int, int>();
            foreach (var e in ActiveEnrollments)
            {
                if (progressMap.ContainsKey(e.ID_cours))
                {
                    continue;
                }

                var p = await _accessDbService.GetCourseProgressPercentForUserAsync(id.Value, e.ID_cours, cancellationToken);
                progressMap[e.ID_cours] = Math.Clamp(p, 0, 100);
            }
            ActiveCourseProgressMap = progressMap;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to calculate active course progress.");
            ActiveCourseProgressMap = new Dictionary<int, int>();
        }

        // «completed» в ENROLLMENTS — устаревшее значение после оплаты; завершение курса только finished/complete.
        FinishedEnrollments = LatestPerCourse(
            Enrollments.Where(e =>
            {
                var st = (e.status ?? string.Empty).Trim().ToLowerInvariant();
                return st is "finished" or "complete";
            }));

        try
        {
            PurchasedCourses = await _accessDbService.GetPurchasedCoursesByUserIdAsync(id.Value, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load purchased courses.");
            PurchasedCourses = [];
        }

        HomeworkSubmissions = ReadHomeworkSubmissionsFromDisk(id.Value);
        try
        {
            var reviews = await _accessDbService.GetHomeworkReviewsByUserIdAsync(id.Value, cancellationToken);
            HomeworkReviewMap = reviews
                .Where(r => r.HomeworkId > 0)
                .GroupBy(r => r.HomeworkId)
                .Select(g => g.OrderByDescending(x => x.ReviewedAt ?? x.SubmittedAt ?? DateTime.MinValue).First())
                .ToDictionary(x => x.HomeworkId, x => x);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load homework reviews for profile.");
            HomeworkReviewMap = new Dictionary<int, HomeworkReviewItem>();
        }

        try
        {
            var rows = new List<HomeworkSubmissionViewRow>();
            var lessonCache = new Dictionary<int, CourseLessonItem?>();
            foreach (var s in HomeworkSubmissions.Take(50))
            {
                HomeworkReviewMap.TryGetValue(s.HomeworkId, out var rev);
                var lessonLabel = "—";
                var lessonId = rev?.LessonId ?? 0;
                if (lessonId <= 0)
                {
                    var lid = await _accessDbService.GetLessonIdByHomeworkIdAsync(s.HomeworkId, cancellationToken);
                    lessonId = lid ?? 0;
                }

                if (lessonId > 0)
                {
                    if (!lessonCache.TryGetValue(lessonId, out var lesson))
                    {
                        lesson = await _accessDbService.GetLessonByIdAsync(lessonId, cancellationToken);
                        lessonCache[lessonId] = lesson;
                    }

                    if (lesson is not null)
                    {
                        var num = lesson.number_lesson;
                        var name = lesson.Cours_name ?? string.Empty;
                        lessonLabel = num.HasValue
                            ? $"Урок №{num.Value}: {name}"
                            : $"Урок #{lessonId}: {name}";
                    }
                    else
                    {
                        lessonLabel = $"Урок ID {lessonId}";
                    }
                }

                rows.Add(new HomeworkSubmissionViewRow
                {
                    Submission = s,
                    Review = rev,
                    LessonLabel = lessonLabel
                });
            }

            HomeworkRows = rows;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to build homework rows with lesson labels.");
            HomeworkRows = [];
        }
    }

    private IReadOnlyList<HomeworkSubmissionItem> ReadHomeworkSubmissionsFromDisk(int userId)
    {
        try
        {
            var root = Path.Combine(_env.WebRootPath, "uploads", "homework", userId.ToString());
            if (!Directory.Exists(root))
            {
                return [];
            }

            var list = new List<HomeworkSubmissionItem>();
            foreach (var hwDir in Directory.EnumerateDirectories(root))
            {
                var hwName = Path.GetFileName(hwDir);
                _ = int.TryParse(hwName, out var homeworkId);

                foreach (var file in Directory.EnumerateFiles(hwDir))
                {
                    var fi = new FileInfo(file);
                    var url = $"/uploads/homework/{userId}/{hwName}/{Uri.EscapeDataString(fi.Name)}";
                    list.Add(new HomeworkSubmissionItem
                    {
                        UserId = userId,
                        LessonId = 0,
                        HomeworkId = homeworkId,
                        FileName = fi.Name,
                        Url = url,
                        UploadedAt = fi.LastWriteTime
                    });
                }
            }

            return list
                .OrderByDescending(x => x.UploadedAt)
                .ToList();
        }
        catch
        {
            return [];
        }
    }

    public async Task<IActionResult> OnPostSaveAsync(int? userId, CancellationToken cancellationToken)
    {
        var id = await ResolveCurrentUserIdAsync(userId, cancellationToken);
        if (id is null)
        {
            return RedirectToPage("/Login");
        }

        if (!ModelState.IsValid)
        {
            await OnGetAsync(id, cancellationToken);
            return Page();
        }

        await _accessDbService.UpdateUserProfileAsync(
            id.Value,
            Input.FullName,
            Input.Email,
            Input.Phone,
            cancellationToken);

        SuccessMessage = "Профиль сохранен.";
        await OnGetAsync(id, cancellationToken);
        return Page();
    }

    public async Task<IActionResult> OnPostUnenrollAsync(int courseId, int? userId, CancellationToken cancellationToken)
    {
        var id = await ResolveCurrentUserIdAsync(userId, cancellationToken);
        if (id is null)
        {
            return RedirectToPage("/Login");
        }

        if (courseId <= 0)
        {
            ErrorMessage = "Некорректный курс.";
            await OnGetAsync(id, cancellationToken);
            return Page();
        }

        var ok = await _accessDbService.SetEnrollmentStatusAsync(id.Value, courseId, "refund_requested", cancellationToken);
        if (!ok)
        {
            ErrorMessage = "Не удалось отправить заявку на отмену.";
            await OnGetAsync(id, cancellationToken);
            return Page();
        }

        SuccessMessage = "Заявка на отмену отправлена. После подтверждения админом курс будет отменён, а деньги вернутся на баланс.";
        await OnGetAsync(id, cancellationToken);
        return Page();
    }

    private async Task<int?> ResolveCurrentUserIdAsync(int? routeUserId, CancellationToken cancellationToken)
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
            var emailClaim = HttpContext.User.FindFirstValue(ClaimTypes.Email);
            var normEmailClaim = string.IsNullOrWhiteSpace(emailClaim) ? null : emailClaim.Trim().Trim('"').ToLowerInvariant();
            var normByIdEmail = string.IsNullOrWhiteSpace(byId?.email) ? null : byId.email.Trim().Trim('"').ToLowerInvariant();
            if (byId is not null && (string.IsNullOrWhiteSpace(emailClaim) ||
                                    string.Equals(normByIdEmail, normEmailClaim, StringComparison.OrdinalIgnoreCase)))
            {
                return claimUserId;
            }
        }

        return null;
    }
}
