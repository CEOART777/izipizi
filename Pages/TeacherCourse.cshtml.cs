using System.Security.Claims;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize(Roles = "teacher")]
public class TeacherCourseModel : PageModel
{
    private readonly AccessDbService _db;
    private readonly IWebHostEnvironment _env;
    private readonly ILogger<TeacherCourseModel> _logger;

    public TeacherCourseModel(AccessDbService db, IWebHostEnvironment env, ILogger<TeacherCourseModel> logger)
    {
        _db = db;
        _env = env;
        _logger = logger;
    }

    [BindProperty(SupportsGet = true)]
    public int CourseId { get; set; }

    [BindProperty(SupportsGet = true)]
    public string? Show { get; set; }

    public CourseItem? Course { get; private set; }

    public IReadOnlyList<CourseLessonItem> Lessons { get; private set; } = [];

    public sealed class StudentRow
    {
        public required UserItem User { get; init; }
        public required IReadOnlyList<HomeworkSubmissionItem> Submissions { get; init; }
        public HomeworkReviewItem? Review { get; init; }
        /// <summary>accepted | rework | submitted | not_sent</summary>
        public string EffectiveStatus { get; init; } = "not_sent";
    }

    public sealed class StudentCompletionRow
    {
        public required UserItem User { get; init; }
        public required int ProgressPercent { get; init; }
        public required bool AllRequiredAccepted { get; init; }
        public required bool CanComplete { get; init; }
    }

    public sealed class LessonBlock
    {
        public required CourseLessonItem Lesson { get; init; }
        public required int HomeworkId { get; init; }
        public string? HomeworkTaskFileName { get; init; }
        public required IReadOnlyList<StudentRow> Students { get; init; }
        public int SubmittedCount { get; init; }
        public int AcceptedCount { get; init; }
        public int ReworkCount { get; init; }
        public int NotSentCount { get; init; }
    }

    public IReadOnlyList<LessonBlock> Blocks { get; private set; } = [];
    public IReadOnlyList<StudentCompletionRow> StudentCompletions { get; private set; } = [];

    public string? ErrorMessage { get; private set; }
    public string? SuccessMessage { get; private set; }

    public async Task<IActionResult> OnGetAsync(CancellationToken cancellationToken)
    {
        // Нормализуем фильтр сразу, чтобы dropdown всегда отражал выбранное значение
        // даже если дальше будет ранний return (ошибка/нет уроков и т.п.).
        var show = (Show ?? "pending").Trim().ToLowerInvariant();
        if (show is not ("all" or "submitted" or "pending" or "reviewed"))
        {
            show = "pending";
        }
        Show = show;

        if (CourseId <= 0)
        {
            ErrorMessage = "Некорректный курс.";
            return Page();
        }

        var teacherId = ResolveCurrentUserId();
        if (teacherId <= 0)
        {
            return RedirectToPage("/Login", new { returnUrl = $"/TeacherCourse?courseId={CourseId}" });
        }

        Course = await _db.GetCourseByIdAsync(CourseId, cancellationToken);
        if (Course is null || !await IsTeacherAssignedToCourseAsync(teacherId, Course, cancellationToken))
        {
            Course = null;
            ErrorMessage = "Курс не найден или не назначен вам.";
            return Page();
        }

        Lessons = await _db.GetLessonsByCourseIdAsync(CourseId, cancellationToken);
        if (Lessons.Count == 0)
        {
            ErrorMessage = "В БД нет уроков у этого курса. Вы можете добавить первый урок ниже.";
            Blocks = [];
            StudentCompletions = [];
            return Page();
        }

        var enrollments = (await _db.GetEnrollmentsByCourseIdAsync(CourseId, cancellationToken))
            .Where(e => !string.Equals((e.status ?? string.Empty).Trim(), "finished", StringComparison.OrdinalIgnoreCase))
            .ToList();
        var usersById = new Dictionary<int, UserItem>();
        foreach (var e in enrollments)
        {
            if (usersById.ContainsKey(e.ID_user))
            {
                continue;
            }
            var u = await _db.GetUserByIdAsync(e.ID_user, cancellationToken);
            if (u is not null)
            {
                usersById[u.ID_user] = u;
            }
        }

        var blocks = new List<LessonBlock>();
        var requiredHomeworkIds = new HashSet<int>();
        foreach (var l in Lessons.OrderBy(x => x.number_lesson ?? int.MaxValue))
        {
            var progress = await _db.GetProgressByLessonIdAsync(l.ID_lesson, cancellationToken);
            var homeworkId = progress?.ID_homework ?? 0;
            if (homeworkId > 0)
            {
                requiredHomeworkIds.Add(homeworkId);
            }

            var rows = new List<StudentRow>();
            var submittedCount = 0;
            var acceptedCount = 0;
            var reworkCount = 0;
            var notSentCount = 0;
            foreach (var u in usersById.Values.OrderBy(x => x.full_name))
            {
                var submissions = homeworkId > 0 ? ReadUserSubmissionsForHomework(u.ID_user, homeworkId) : [];
                var review = homeworkId > 0 ? await _db.GetHomeworkReviewAsync(u.ID_user, homeworkId, cancellationToken) : null;

                var status = (review?.Status ?? string.Empty).Trim().ToLowerInvariant();
                var effectiveStatus =
                    status is "accepted" or "rework" or "submitted"
                        ? status
                        : (submissions.Count > 0 ? "submitted" : "not_sent");
                var hasAny = submissions.Count > 0 || !string.IsNullOrWhiteSpace(status);

                if (string.Equals(effectiveStatus, "accepted", StringComparison.OrdinalIgnoreCase))
                {
                    acceptedCount++;
                }
                else if (string.Equals(effectiveStatus, "rework", StringComparison.OrdinalIgnoreCase))
                {
                    reworkCount++;
                }
                else if (string.Equals(effectiveStatus, "submitted", StringComparison.OrdinalIgnoreCase))
                {
                    submittedCount++;
                }
                else if (hasAny)
                {
                    // неизвестный статус (на будущее)
                }
                else
                {
                    notSentCount++;
                }

                var include = show switch
                {
                    "all" => true,
                    "submitted" => string.Equals(effectiveStatus, "submitted", StringComparison.OrdinalIgnoreCase),
                    "pending" => string.Equals(effectiveStatus, "submitted", StringComparison.OrdinalIgnoreCase),
                    "reviewed" => string.Equals(effectiveStatus, "accepted", StringComparison.OrdinalIgnoreCase) || string.Equals(effectiveStatus, "rework", StringComparison.OrdinalIgnoreCase),
                    _ => true
                };
                if (!include)
                {
                    continue;
                }

                rows.Add(new StudentRow
                {
                    User = u,
                    Submissions = submissions,
                    Review = review,
                    EffectiveStatus = effectiveStatus
                });
            }

            blocks.Add(new LessonBlock
            {
                Lesson = l,
                HomeworkId = homeworkId,
                HomeworkTaskFileName = GetHomeworkTaskFileName(l.ID_lesson),
                Students = rows,
                SubmittedCount = submittedCount,
                AcceptedCount = acceptedCount,
                ReworkCount = reworkCount,
                NotSentCount = notSentCount
            });
        }

        Blocks = blocks;

        try
        {
            var completionRows = new List<StudentCompletionRow>();
            foreach (var u in usersById.Values.OrderBy(x => x.full_name))
            {
                var progressPercent = await _db.GetCourseProgressPercentForUserAsync(u.ID_user, CourseId, cancellationToken);
                var allAccepted = true;
                if (requiredHomeworkIds.Count > 0)
                {
                    foreach (var hwId in requiredHomeworkIds)
                    {
                        var r = await _db.GetHomeworkReviewAsync(u.ID_user, hwId, cancellationToken);
                        if (!string.Equals((r?.Status ?? string.Empty).Trim(), "accepted", StringComparison.OrdinalIgnoreCase))
                        {
                            allAccepted = false;
                            break;
                        }
                    }
                }

                // Завершение по кнопке — по факту прогресса 100% (после проверок ДЗ); галочки «все приняты» — подсказка.
                var canComplete = progressPercent >= 100;
                completionRows.Add(new StudentCompletionRow
                {
                    User = u,
                    ProgressPercent = progressPercent,
                    AllRequiredAccepted = allAccepted,
                    CanComplete = canComplete
                });
            }

            StudentCompletions = completionRows;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to build completion rows.");
            StudentCompletions = [];
        }

        return Page();
    }

    public async Task<IActionResult> OnPostCompleteAsync(
        int courseId,
        int userId,
        string? show,
        CancellationToken cancellationToken)
    {
        CourseId = courseId;
        Show = show;

        var teacherId = ResolveCurrentUserId();
        if (teacherId <= 0)
        {
            return RedirectToPage("/Login", new { returnUrl = $"/TeacherCourse?courseId={courseId}" });
        }

        if (courseId <= 0 || userId <= 0)
        {
            ErrorMessage = "Некорректные данные формы.";
            return await OnGetAsync(cancellationToken);
        }

        Course = await _db.GetCourseByIdAsync(courseId, cancellationToken);
        if (Course is null || !await IsTeacherAssignedToCourseAsync(teacherId, Course, cancellationToken))
        {
            ErrorMessage = "Курс не найден или не назначен вам.";
            Course = null;
            return Page();
        }

        // Проверяем, что действительно все ДЗ приняты и прогресс 100%.
        var lessons = await _db.GetLessonsByCourseIdAsync(courseId, cancellationToken);
        var requiredHomeworkIds = new HashSet<int>();
        foreach (var l in lessons)
        {
            var progress = await _db.GetProgressByLessonIdAsync(l.ID_lesson, cancellationToken);
            var hwId = progress?.ID_homework ?? 0;
            if (hwId > 0) requiredHomeworkIds.Add(hwId);
        }

        var progressPercent = await _db.GetCourseProgressPercentForUserAsync(userId, courseId, cancellationToken);
        if (progressPercent < 100)
        {
            ErrorMessage = "Нельзя завершить обучение: прогресс курса меньше 100%.";
            return await OnGetAsync(cancellationToken);
        }

        await _db.EnsureEnrollmentStatusColumnAsync(cancellationToken);
        var ok = await _db.SetEnrollmentStatusAsync(userId, courseId, "finished", cancellationToken);
        SuccessMessage = ok ? "Обучение завершено для пользователя." : "Не удалось завершить обучение (нет столбца status или запись ENROLLMENTS не найдена).";

        var showValue = (Show ?? "pending").Trim().ToLowerInvariant();
        if (showValue is not ("all" or "submitted" or "pending" or "reviewed"))
        {
            showValue = "pending";
        }
        return RedirectToPage("/TeacherCourse", new { courseId, show = showValue });
    }

    public async Task<IActionResult> OnPostAsync(
        int courseId,
        int userId,
        int homeworkId,
        int lessonId,
        string status,
        int? grade,
        string? show,
        CancellationToken cancellationToken)
    {
        CourseId = courseId;
        Show = show;

        var teacherId = ResolveCurrentUserId();
        if (teacherId <= 0)
        {
            return RedirectToPage("/Login", new { returnUrl = $"/TeacherCourse?courseId={courseId}" });
        }

        if (courseId <= 0 || userId <= 0 || homeworkId <= 0)
        {
            ErrorMessage = "Некорректные данные формы.";
            return await OnGetAsync(cancellationToken);
        }

        Course = await _db.GetCourseByIdAsync(courseId, cancellationToken);
        if (Course is null || !await IsTeacherAssignedToCourseAsync(teacherId, Course, cancellationToken))
        {
            ErrorMessage = "Курс не найден или не назначен вам.";
            Course = null;
            return Page();
        }

        status = (status ?? string.Empty).Trim().ToLowerInvariant();
        if (status is not ("accepted" or "rework"))
        {
            ErrorMessage = "Некорректный статус проверки.";
            return await OnGetAsync(cancellationToken);
        }

        if (grade.HasValue && (grade.Value < 1 || grade.Value > 5))
        {
            ErrorMessage = "Оценка должна быть от 1 до 5.";
            return await OnGetAsync(cancellationToken);
        }

        try
        {
            await _db.SetHomeworkReviewAsync(
                teacherId,
                userId,
                courseId,
                lessonId,
                homeworkId,
                status,
                grade,
                DateTime.Now,
                cancellationToken);

            SuccessMessage = "Проверка сохранена.";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save homework review.");
            ErrorMessage = "Не удалось сохранить проверку.";
        }

        var showValue = (Show ?? "pending").Trim().ToLowerInvariant();
        if (showValue is not ("all" or "submitted" or "pending" or "reviewed"))
        {
            showValue = "pending";
        }
        return RedirectToPage("/TeacherCourse", new { courseId, show = showValue });
    }

    public async Task<IActionResult> OnPostDeleteLessonAsync(
        int courseId,
        int lessonId,
        string? show,
        CancellationToken cancellationToken)
    {
        CourseId = courseId;
        Show = show;

        var teacherId = ResolveCurrentUserId();
        if (teacherId <= 0)
        {
            return RedirectToPage("/Login", new { returnUrl = $"/TeacherCourse?courseId={courseId}" });
        }

        if (courseId <= 0 || lessonId <= 0)
        {
            ErrorMessage = "Некорректные данные удаления урока.";
            return await OnGetAsync(cancellationToken);
        }

        var course = await _db.GetCourseByIdAsync(courseId, cancellationToken);
        if (course is null || !await IsTeacherAssignedToCourseAsync(teacherId, course, cancellationToken))
        {
            return Forbid();
        }

        var lesson = await _db.GetLessonByIdAsync(lessonId, cancellationToken);
        if (lesson is null || lesson.ID_cours != courseId)
        {
            ErrorMessage = "Урок не найден в этом курсе.";
            return await OnGetAsync(cancellationToken);
        }

        try
        {
            var deleted = await _db.DeleteLessonAsync(lessonId, cancellationToken);
            if (!deleted)
            {
                ErrorMessage = "Не удалось удалить урок.";
                return await OnGetAsync(cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete lesson from teacher course page.");
            ErrorMessage = "Ошибка при удалении урока.";
            return await OnGetAsync(cancellationToken);
        }

        return RedirectToPage("/TeacherCourse", new { courseId, show = "all" });
    }

    public async Task<IActionResult> OnPostUploadHomeworkTaskAsync(
        int courseId,
        int lessonId,
        IFormFile? file,
        string? show,
        CancellationToken cancellationToken)
    {
        CourseId = courseId;
        Show = show;

        var teacherId = ResolveCurrentUserId();
        if (teacherId <= 0)
        {
            return RedirectToPage("/Login", new { returnUrl = $"/TeacherCourse?courseId={courseId}" });
        }

        if (courseId <= 0 || lessonId <= 0)
        {
            ErrorMessage = "Некорректные данные для ДЗ.";
            return await OnGetAsync(cancellationToken);
        }

        var course = await _db.GetCourseByIdAsync(courseId, cancellationToken);
        if (course is null || !await IsTeacherAssignedToCourseAsync(teacherId, course, cancellationToken))
        {
            return Forbid();
        }

        var lesson = await _db.GetLessonByIdAsync(lessonId, cancellationToken);
        if (lesson is null || lesson.ID_cours != courseId)
        {
            ErrorMessage = "Урок не найден в выбранном курсе.";
            return await OnGetAsync(cancellationToken);
        }

        if (file is null || file.Length <= 0)
        {
            ErrorMessage = "Выберите файл с заданием.";
            return await OnGetAsync(cancellationToken);
        }

        try
        {
            // Если у урока ещё нет записи ДЗ в HOMEWORK/PROGRES, создаём её автоматически.
            _ = await _db.UpsertHomeworkForLessonAsync(lessonId, null, cancellationToken);

            var ext = Path.GetExtension(file.FileName);
            var baseName = Path.GetFileNameWithoutExtension(file.FileName);
            if (string.IsNullOrWhiteSpace(baseName))
            {
                baseName = "homework_task";
            }
            var safeBase = string.Join("_", baseName.Split(Path.GetInvalidFileNameChars(), StringSplitOptions.RemoveEmptyEntries)).Trim();
            if (string.IsNullOrWhiteSpace(safeBase))
            {
                safeBase = "homework_task";
            }
            var stamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var finalFileName = $"{stamp}_{safeBase}{ext}";

            var dir = Path.Combine(_env.WebRootPath, "uploads", "homework_tasks", lessonId.ToString());
            Directory.CreateDirectory(dir);

            foreach (var old in Directory.EnumerateFiles(dir))
            {
                System.IO.File.Delete(old);
            }

            var path = Path.Combine(dir, finalFileName);
            await using (var stream = System.IO.File.Create(path))
            {
                await file.CopyToAsync(stream, cancellationToken);
            }

            SuccessMessage = "Файл домашнего задания загружен.";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to upload homework task file by teacher.");
            ErrorMessage = "Не удалось загрузить файл ДЗ.";
        }

        var showValue = (Show ?? "all").Trim().ToLowerInvariant();
        if (showValue is not ("all" or "submitted" or "pending" or "reviewed"))
        {
            showValue = "all";
        }
        return RedirectToPage("/TeacherCourse", new { courseId, show = showValue });
    }

    public async Task<IActionResult> OnPostCreateLessonAsync(
        int courseId,
        string? lessonName,
        int? lessonNumber,
        string? show,
        CancellationToken cancellationToken)
    {
        CourseId = courseId;
        Show = show;

        var teacherId = ResolveCurrentUserId();
        if (teacherId <= 0)
        {
            return RedirectToPage("/Login", new { returnUrl = $"/TeacherCourse?courseId={courseId}" });
        }

        if (courseId <= 0)
        {
            ErrorMessage = "Некорректный курс.";
            return await OnGetAsync(cancellationToken);
        }

        var course = await _db.GetCourseByIdAsync(courseId, cancellationToken);
        if (course is null || !await IsTeacherAssignedToCourseAsync(teacherId, course, cancellationToken))
        {
            return Forbid();
        }

        var title = string.IsNullOrWhiteSpace(lessonName) ? $"Новый урок {DateTime.Now:HH:mm:ss}" : lessonName.Trim();
        try
        {
            _ = await _db.CreateLessonAdminAsync(
                courseId,
                lessonNumber,
                title,
                string.Empty,
                string.Empty,
                string.Empty,
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Teacher failed to create lesson.");
            ErrorMessage = "Не удалось добавить урок.";
            return await OnGetAsync(cancellationToken);
        }

        return RedirectToPage("/TeacherCourse", new { courseId, show = "all" });
    }

    private int ResolveCurrentUserId()
    {
        var claimValue = HttpContext.User.FindFirstValue(ClaimTypes.NameIdentifier);
        return int.TryParse(claimValue, out var id) ? id : 0;
    }

    private async Task<bool> IsTeacherAssignedToCourseAsync(int teacherId, CourseItem course, CancellationToken cancellationToken)
    {
        if (teacherId <= 0 || course.ID_curs <= 0)
        {
            return false;
        }

        if (course.teacher_user_id == teacherId)
        {
            return true;
        }

        // Fallback: назначение может храниться в TEACHER_COURSES
        var ids = await _db.GetAssignedCourseIdsForTeacherAsync(teacherId, cancellationToken);
        return ids.Contains(course.ID_curs);
    }

    private IReadOnlyList<HomeworkSubmissionItem> ReadUserSubmissionsForHomework(int userId, int homeworkId)
    {
        try
        {
            var dir = Path.Combine(_env.WebRootPath, "uploads", "homework", userId.ToString(), homeworkId.ToString());
            if (!Directory.Exists(dir))
            {
                return [];
            }

            var list = new List<HomeworkSubmissionItem>();
            foreach (var file in Directory.EnumerateFiles(dir))
            {
                var fi = new FileInfo(file);
                var url = $"/uploads/homework/{userId}/{homeworkId}/{Uri.EscapeDataString(fi.Name)}";
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

            return list
                .OrderByDescending(x => x.UploadedAt)
                .ToList();
        }
        catch
        {
            return [];
        }
    }

    private string? GetHomeworkTaskFileName(int lessonId)
    {
        if (lessonId <= 0)
        {
            return null;
        }

        try
        {
            var dir = Path.Combine(_env.WebRootPath, "uploads", "homework_tasks", lessonId.ToString());
            if (!Directory.Exists(dir))
            {
                return null;
            }

            var latest = Directory.EnumerateFiles(dir)
                .Select(p => new FileInfo(p))
                .OrderByDescending(f => f.LastWriteTimeUtc)
                .FirstOrDefault();

            return latest?.Name;
        }
        catch
        {
            return null;
        }
    }

    // lessonId теперь выбирается из списка уроков курса
}

