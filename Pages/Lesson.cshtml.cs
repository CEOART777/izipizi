using System.Security.Claims;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.StaticFiles;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

public class LessonModel : PageModel
{
    private const string LastCourseCookie = "last_course_id";
    private const string LastLessonCookie = "last_lesson_id";
    private readonly AccessDbService _accessDbService;
    private readonly ILogger<LessonModel> _logger;
    private readonly IWebHostEnvironment _env;
    private readonly IHttpClientFactory _httpClientFactory;

    public LessonModel(
        AccessDbService accessDbService,
        ILogger<LessonModel> logger,
        IWebHostEnvironment env,
        IHttpClientFactory httpClientFactory)
    {
        _accessDbService = accessDbService;
        _logger = logger;
        _env = env;
        _httpClientFactory = httpClientFactory;
    }

    public CourseLessonItem? Lesson { get; private set; }
    public LessonProgressItem? Progress { get; private set; }
    public HomeworkReviewItem? HomeworkReview { get; private set; }
    public int CourseProgressPercent { get; private set; }
    public IReadOnlyList<CourseLessonItem> CoursePlaylist { get; private set; } = [];
    public string CourseTitle { get; private set; } = string.Empty;
    public int? LessonNumber { get; private set; }

    /// <summary>Запрошен /Lesson?courseId=…, но в БД нет ни одного урока у этого курса.</summary>
    public bool CourseHasNoLessons { get; private set; }

    /// <summary>Курс из query, если открытие шло по courseId.</summary>
    public int? RequestedCourseId { get; private set; }

    public string? HomeworkUploadMessage { get; private set; }

    /// <summary>Запись ENROLLMENTS со статусом finished — загрузка ДЗ отключена, просмотр уроков возможен.</summary>
    public bool CourseEnrollmentFinished { get; private set; }

    public string? MaterialsHref { get; private set; }
    public bool MaterialsIsLocalFile { get; private set; }
    public string? MaterialsFileName { get; private set; }
    public bool MaterialsCanInlineView { get; private set; }
    public bool MaterialsIsExternalLink { get; private set; }
    public string? HomeworkTaskHref { get; private set; }
    public string? HomeworkTaskFileName { get; private set; }

    public async Task<IActionResult> OnGetAsync(int? lessonId, int? courseId, CancellationToken cancellationToken)
    {
        var id = await ResolveLessonIdAsync(lessonId, courseId, cancellationToken);

        try
        {
            if (id < 0)
            {
                CourseHasNoLessons = true;
                RequestedCourseId = courseId;
                if (courseId.HasValue && courseId.Value > 0)
                {
                    var c = await _accessDbService.GetCourseByIdAsync(courseId.Value, cancellationToken);
                    CourseTitle = c?.Course_name ?? string.Empty;
                }

                return Page();
            }

            await LoadLessonContextAsync(id, cancellationToken);
            if (Lesson is null)
            {
                await LoadFirstAvailableLessonAsync(cancellationToken);
            }

            // Доступ к урокам только после записи/оплаты (для админа/учителя доступ оставляем)
            if (Lesson is not null && !(User.IsInRole("admin") || User.IsInRole("teacher")))
            {
                var claimValue = HttpContext.User.FindFirstValue(ClaimTypes.NameIdentifier);
                if (!int.TryParse(claimValue, out var userId) || userId <= 0)
                {
                    // не авторизован — отправляем на логин, затем на запись
                    return RedirectToPage("/Login", new { returnUrl = $"/Enroll?courseId={Lesson.ID_cours}" });
                }

                var enrolled = await _accessDbService.IsUserEnrolledInCourseAsync(userId, Lesson.ID_cours, cancellationToken);
                if (!enrolled)
                {
                    return RedirectToPage("/Enroll", new { courseId = Lesson.ID_cours });
                }
            }

            await LoadCurrentUserHomeworkReviewAsync(cancellationToken);
            BuildMaterialsLink();
            BuildHomeworkTaskLink();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load lesson page data.");
            Lesson = null;
            Progress = null;
            HomeworkReview = null;
            CoursePlaylist = [];
            CourseTitle = string.Empty;
            LessonNumber = null;
            CourseHasNoLessons = false;
            RequestedCourseId = null;
            MaterialsHref = null;
            MaterialsIsLocalFile = false;
            MaterialsFileName = null;
            HomeworkTaskHref = null;
            HomeworkTaskFileName = null;
        }

        return Page();
    }

    private async Task LoadCurrentUserHomeworkReviewAsync(CancellationToken cancellationToken)
    {
        HomeworkReview = null;

        var homeworkId = Progress?.ID_homework ?? 0;
        if (homeworkId <= 0)
        {
            return;
        }

        var claimValue = HttpContext.User.FindFirstValue(ClaimTypes.NameIdentifier);
        if (!int.TryParse(claimValue, out var userId) || userId <= 0)
        {
            return;
        }

        HomeworkReview = await _accessDbService.GetHomeworkReviewAsync(userId, homeworkId, cancellationToken);
    }

    public async Task<IActionResult> OnGetMaterialAsync(int lessonId, CancellationToken cancellationToken)
    {
        if (lessonId <= 0)
        {
            return NotFound();
        }

        var lesson = await _accessDbService.GetLessonByIdAsync(lessonId, cancellationToken);
        if (lesson is null)
        {
            return NotFound();
        }

        if (!(User.IsInRole("admin") || User.IsInRole("teacher")))
        {
            var claimValue = HttpContext.User.FindFirstValue(ClaimTypes.NameIdentifier);
            if (!int.TryParse(claimValue, out var userId) || userId <= 0)
            {
                return Forbid();
            }
            var enrolled = await _accessDbService.IsUserEnrolledInCourseAsync(userId, lesson.ID_cours, cancellationToken);
            if (!enrolled)
            {
                return Forbid();
            }
        }

        var raw = lesson.meterials_url;

        var localPath = ExtractLocalPath(raw);
        if (!string.IsNullOrWhiteSpace(localPath) && System.IO.File.Exists(localPath))
        {
            return InlinePhysical(localPath);
        }

        var webPath = ExtractWebRootRelativePath(raw);
        if (!string.IsNullOrWhiteSpace(webPath))
        {
            var physical = Path.Combine(_env.WebRootPath, webPath.TrimStart('/').Replace('/', Path.DirectorySeparatorChar));
            if (System.IO.File.Exists(physical))
            {
                return InlinePhysical(physical);
            }
        }

        var url = ExtractHttpUrl(raw);
        if (!string.IsNullOrWhiteSpace(url) && Uri.TryCreate(url, UriKind.Absolute, out var uri) && uri.Scheme is ("http" or "https"))
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(30);

            using var resp = await client.GetAsync(uri, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            if (!resp.IsSuccessStatusCode)
            {
                return StatusCode((int)resp.StatusCode);
            }

            const long maxBytes = 25L * 1024 * 1024; // 25 MB
            var contentLength = resp.Content.Headers.ContentLength;
            if (contentLength is > maxBytes)
            {
                return BadRequest("Файл слишком большой для открытия через платформу.");
            }

            var contentType = resp.Content.Headers.ContentType?.ToString() ?? "application/octet-stream";
            var fileName =
                resp.Content.Headers.ContentDisposition?.FileNameStar ??
                resp.Content.Headers.ContentDisposition?.FileName ??
                Path.GetFileName(uri.LocalPath);
            fileName = (fileName ?? "materials").Trim().Trim('"');
            if (string.IsNullOrWhiteSpace(fileName))
            {
                fileName = "materials";
            }

            Response.Headers.ContentDisposition = $"inline; filename*=UTF-8''{Uri.EscapeDataString(fileName)}";
            await using var remote = await resp.Content.ReadAsStreamAsync(cancellationToken);
            var ms = new MemoryStream();
            await remote.CopyToAsync(ms, cancellationToken);
            ms.Position = 0;
            return File(ms, contentType);
        }

        return NotFound();
    }

    public async Task<IActionResult> OnGetMaterialDownloadAsync(int lessonId, CancellationToken cancellationToken)
    {
        if (lessonId <= 0)
        {
            return NotFound();
        }

        var lesson = await _accessDbService.GetLessonByIdAsync(lessonId, cancellationToken);
        if (lesson is null)
        {
            return NotFound();
        }

        if (!(User.IsInRole("admin") || User.IsInRole("teacher")))
        {
            var claimValue = HttpContext.User.FindFirstValue(ClaimTypes.NameIdentifier);
            if (!int.TryParse(claimValue, out var userId) || userId <= 0)
            {
                return Forbid();
            }
            var enrolled = await _accessDbService.IsUserEnrolledInCourseAsync(userId, lesson.ID_cours, cancellationToken);
            if (!enrolled)
            {
                return Forbid();
            }
        }

        var raw = lesson.meterials_url;

        var localPath = ExtractLocalPath(raw);
        if (!string.IsNullOrWhiteSpace(localPath) && System.IO.File.Exists(localPath))
        {
            return DownloadPhysical(localPath);
        }

        var webPath = ExtractWebRootRelativePath(raw);
        if (!string.IsNullOrWhiteSpace(webPath))
        {
            var physical = Path.Combine(_env.WebRootPath, webPath.TrimStart('/').Replace('/', Path.DirectorySeparatorChar));
            if (System.IO.File.Exists(physical))
            {
                return DownloadPhysical(physical);
            }
        }

        var url = ExtractHttpUrl(raw);
        if (!string.IsNullOrWhiteSpace(url) && Uri.TryCreate(url, UriKind.Absolute, out var uri) && uri.Scheme is ("http" or "https"))
        {
            // используем уже готовый handler для скачивания по ссылке
            return await OnGetMaterialRemoteDownloadAsync(lessonId, cancellationToken);
        }

        return NotFound();
    }

    [Authorize]
    public async Task<IActionResult> OnPostUploadHomeworkAsync(int lessonId, int homeworkId, IFormFile? file, CancellationToken cancellationToken)
    {
        if (lessonId <= 0 || homeworkId <= 0)
        {
            return RedirectToPage("/Lesson", new { lessonId });
        }

        if (file is null || file.Length <= 0)
        {
            HomeworkUploadMessage = "Выберите файл для загрузки.";
            _ = await OnGetAsync(lessonId, null, cancellationToken);
            return Page();
        }

        var userIdValue = HttpContext.User.FindFirstValue(ClaimTypes.NameIdentifier);
        if (!int.TryParse(userIdValue, out var userId) || userId <= 0)
        {
            return RedirectToPage("/Login", new { returnUrl = $"/Lesson?lessonId={lessonId}#lesson-pane-homework" });
        }

        var lessonForCourse = await _accessDbService.GetLessonByIdAsync(lessonId, cancellationToken);
        if (lessonForCourse is null)
        {
            return RedirectToPage("/Lesson", new { lessonId });
        }

        if (!(User.IsInRole("admin") || User.IsInRole("teacher")))
        {
            var canSubmit = await _accessDbService.CanUserSubmitHomeworkAsync(userId, lessonForCourse.ID_cours, cancellationToken);
            if (!canSubmit)
            {
                HomeworkUploadMessage = "Обучение по этому курсу завершено; отправка домашних заданий недоступна.";
                _ = await OnGetAsync(lessonId, null, cancellationToken);
                return Page();
            }
        }

        var safeName = Path.GetFileName(file.FileName);
        if (string.IsNullOrWhiteSpace(safeName))
        {
            safeName = "homework_file";
        }

        var stamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        var folder = Path.Combine(_env.WebRootPath, "uploads", "homework", userId.ToString(), homeworkId.ToString());
        Directory.CreateDirectory(folder);
        var savedFileName = $"{stamp}_{safeName}";
        var fullPath = Path.Combine(folder, savedFileName);

        await using (var stream = System.IO.File.Create(fullPath))
        {
            await file.CopyToAsync(stream, cancellationToken);
        }

        // фиксируем статус сдачи для конкретного пользователя (учитель потом проверит)
        var courseId = lessonForCourse.ID_cours;
        if (courseId > 0)
        {
            await _accessDbService.UpsertHomeworkSubmittedAsync(
                userId,
                courseId,
                lessonId,
                homeworkId,
                DateTime.Now,
                cancellationToken);
        }

        HomeworkUploadMessage = "Файл успешно загружен.";
        return Redirect($"/Lesson?lessonId={lessonId}#lesson-pane-homework");
    }

    public async Task<IActionResult> OnGetHomeworkTaskAsync(int lessonId, CancellationToken cancellationToken)
    {
        if (lessonId <= 0)
        {
            return NotFound();
        }

        var lesson = await _accessDbService.GetLessonByIdAsync(lessonId, cancellationToken);
        if (lesson is null)
        {
            return NotFound();
        }

        if (!(User.IsInRole("admin") || User.IsInRole("teacher")))
        {
            var claimValue = HttpContext.User.FindFirstValue(ClaimTypes.NameIdentifier);
            if (!int.TryParse(claimValue, out var userId) || userId <= 0)
            {
                return Forbid();
            }

            var enrolled = await _accessDbService.IsUserEnrolledInCourseAsync(userId, lesson.ID_cours, cancellationToken);
            if (!enrolled)
            {
                return Forbid();
            }
        }

        var dir = Path.Combine(_env.WebRootPath, "uploads", "homework_tasks", lessonId.ToString());
        if (!Directory.Exists(dir))
        {
            return NotFound();
        }

        var latest = Directory.EnumerateFiles(dir)
            .Select(p => new FileInfo(p))
            .OrderByDescending(f => f.LastWriteTimeUtc)
            .FirstOrDefault();
        if (latest is null || !latest.Exists)
        {
            return NotFound();
        }

        return DownloadPhysical(latest.FullName);
    }

    private void BuildMaterialsLink()
    {
        MaterialsHref = null;
        MaterialsIsLocalFile = false;
        MaterialsFileName = null;
        MaterialsCanInlineView = false;
        MaterialsIsExternalLink = false;

        var raw = Lesson?.meterials_url;
        if (string.IsNullOrWhiteSpace(raw))
        {
            return;
        }

        var s = raw.Trim();
        var lessonId = Lesson?.ID_lesson ?? 0;
        MaterialsHref = lessonId > 0 ? $"/Lesson?handler=Material&lessonId={lessonId}" : null;

        var url = ExtractHttpUrl(s);
        if (!string.IsNullOrWhiteSpace(url))
        {
            MaterialsIsExternalLink = true;
            MaterialsCanInlineView = CanInlineViewByExtension(url);
            return;
        }

        var localPath = ExtractLocalPath(s);
        if (!string.IsNullOrWhiteSpace(localPath))
        {
            MaterialsIsLocalFile = true;
            MaterialsFileName = Path.GetFileName(localPath);
            MaterialsCanInlineView = CanInlineViewByExtension(MaterialsFileName);
            return;
        }

        var webPath = ExtractWebRootRelativePath(s);
        if (!string.IsNullOrWhiteSpace(webPath))
        {
            MaterialsFileName = Path.GetFileName(webPath);
            MaterialsCanInlineView = CanInlineViewByExtension(webPath);
        }
    }

    private void BuildHomeworkTaskLink()
    {
        HomeworkTaskHref = null;
        HomeworkTaskFileName = null;

        var lessonId = Lesson?.ID_lesson ?? 0;
        if (lessonId <= 0)
        {
            return;
        }

        var dir = Path.Combine(_env.WebRootPath, "uploads", "homework_tasks", lessonId.ToString());
        if (!Directory.Exists(dir))
        {
            return;
        }

        var latest = Directory.EnumerateFiles(dir)
            .Select(p => new FileInfo(p))
            .OrderByDescending(f => f.LastWriteTimeUtc)
            .FirstOrDefault();
        if (latest is null)
        {
            return;
        }

        HomeworkTaskFileName = latest.Name;
        HomeworkTaskHref = $"/Lesson?handler=HomeworkTask&lessonId={lessonId}";
    }

    public async Task<IActionResult> OnGetMaterialRemoteDownloadAsync(int lessonId, CancellationToken cancellationToken)
    {
        if (lessonId <= 0)
        {
            return NotFound();
        }

        var lesson = await _accessDbService.GetLessonByIdAsync(lessonId, cancellationToken);
        if (lesson is null)
        {
            return NotFound();
        }

        var raw = lesson.meterials_url?.Trim();
        if (string.IsNullOrWhiteSpace(raw))
        {
            return NotFound();
        }

        // допускаем мусор вокруг, но скачиваем только http(s)
        var httpsIdx = raw.IndexOf("https://", StringComparison.OrdinalIgnoreCase);
        var httpIdx = raw.IndexOf("http://", StringComparison.OrdinalIgnoreCase);
        var idx = httpsIdx >= 0 ? httpsIdx : httpIdx;
        if (idx < 0)
        {
            return BadRequest("Материалы не являются ссылкой.");
        }

        var url = raw[idx..].Trim().Trim('"');
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri) || (uri.Scheme is not ("http" or "https")))
        {
            return BadRequest("Некорректная ссылка на материалы.");
        }

        var client = _httpClientFactory.CreateClient();
        client.Timeout = TimeSpan.FromSeconds(30);

        using var resp = await client.GetAsync(uri, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        if (!resp.IsSuccessStatusCode)
        {
            return StatusCode((int)resp.StatusCode);
        }

        const long maxBytes = 25L * 1024 * 1024; // 25 MB
        var contentLength = resp.Content.Headers.ContentLength;
        if (contentLength is > maxBytes)
        {
            return BadRequest("Файл слишком большой для скачивания через платформу.");
        }

        var contentType = resp.Content.Headers.ContentType?.ToString() ?? "application/octet-stream";

        var fileName =
            resp.Content.Headers.ContentDisposition?.FileNameStar ??
            resp.Content.Headers.ContentDisposition?.FileName ??
            Path.GetFileName(uri.LocalPath);
        fileName = (fileName ?? "materials").Trim().Trim('"');
        if (string.IsNullOrWhiteSpace(fileName))
        {
            fileName = "materials";
        }

        await using var remote = await resp.Content.ReadAsStreamAsync(cancellationToken);
        var ms = new MemoryStream();
        await remote.CopyToAsync(ms, cancellationToken);
        ms.Position = 0;
        return File(ms, contentType, fileName);
    }

    private static bool CanInlineViewByExtension(string? pathOrFileName)
    {
        var ext = Path.GetExtension(pathOrFileName ?? string.Empty).ToLowerInvariant();
        return ext is ".pdf" or ".png" or ".jpg" or ".jpeg" or ".gif" or ".webp" or ".txt";
    }

    private static string? ExtractLocalPath(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        static string StripQuotes(string s)
        {
            s = (s ?? string.Empty).Trim();
            s = s.Trim().Trim('"').Trim('\'').Trim();
            while ((s.StartsWith("\"") && s.EndsWith("\"") && s.Length > 1) ||
                   (s.StartsWith("'") && s.EndsWith("'") && s.Length > 1))
            {
                s = s[1..^1].Trim();
            }
            return s;
        }

        var s = StripQuotes(raw);

        // file:///D:/path/file.ext
        if (s.StartsWith("file://", StringComparison.OrdinalIgnoreCase))
        {
            s = s["file://".Length..].TrimStart('/');
            s = StripQuotes(s);
        }

        // Если в строке есть мусор вокруг пути — пытаемся вырезать "D:\..." или "\\server\share\..."
        // 1) UNC
        var uncIdx = s.IndexOf(@"\\", StringComparison.Ordinal);
        if (uncIdx >= 0)
        {
            var candidate = StripQuotes(s[uncIdx..]);
            candidate = candidate.Split(['\r', '\n', '\t'], StringSplitOptions.RemoveEmptyEntries)[0].Trim();
            return candidate;
        }

        // 2) Drive path anywhere in string
        for (var i = 0; i + 2 < s.Length; i++)
        {
            if (char.IsLetter(s[i]) && s[i + 1] == ':' && (s[i + 2] == '\\' || s[i + 2] == '/'))
            {
                var candidate = StripQuotes(s[i..]);
                candidate = candidate.Split(['\r', '\n', '\t'], StringSplitOptions.RemoveEmptyEntries)[0].Trim();
                // отрезаем возможные хвосты после пробела (редко, но бывает)
                var space = candidate.IndexOf(' ');
                if (space > 0)
                {
                    candidate = candidate[..space];
                }
                return candidate.Replace('/', '\\');
            }
        }

        return null;
    }

    private static string? ExtractHttpUrl(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        var s = raw.Trim();
        var httpsIdx = s.IndexOf("https://", StringComparison.OrdinalIgnoreCase);
        var httpIdx = s.IndexOf("http://", StringComparison.OrdinalIgnoreCase);
        var idx = httpsIdx >= 0 ? httpsIdx : httpIdx;
        if (idx < 0)
        {
            return null;
        }

        var tail = s[idx..].Trim().Trim('"').Trim('\'').Trim();
        // отрезаем после пробела/перевода строки (если в поле лежит "https://... что-то")
        var cut = tail.IndexOfAny([' ', '\r', '\n', '\t']);
        if (cut > 0)
        {
            tail = tail[..cut].Trim();
        }
        // иногда ссылка заканчивается кавычкой или точкой
        tail = tail.TrimEnd('"', '\'', '.', ',', ';');
        return tail;
    }

    private static string? ExtractWebRootRelativePath(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        var s = raw.Trim().Trim('"').Trim('\'').Trim();
        s = s.Replace('\\', '/').TrimStart('~').Trim();

        // если это похоже на "D:\..." или "C:\..." — это не wwwroot path
        if (s.Length >= 2 && char.IsLetter(s[0]) && s[1] == ':')
        {
            return null;
        }
        if (s.StartsWith("file://", StringComparison.OrdinalIgnoreCase) || s.StartsWith(@"\\", StringComparison.Ordinal))
        {
            return null;
        }

        if (!s.StartsWith("/"))
        {
            s = "/" + s;
        }

        return s.StartsWith("/") ? s : null;
    }

    private IActionResult InlinePhysical(string physicalPath)
    {
        var fileName = Path.GetFileName(physicalPath);
        var provider = new FileExtensionContentTypeProvider();
        if (!provider.TryGetContentType(fileName, out var contentType))
        {
            contentType = "application/octet-stream";
        }

        Response.Headers.ContentDisposition = $"inline; filename*=UTF-8''{Uri.EscapeDataString(fileName)}";
        return PhysicalFile(physicalPath, contentType);
    }

    private IActionResult DownloadPhysical(string physicalPath)
    {
        var fileName = Path.GetFileName(physicalPath);
        var provider = new FileExtensionContentTypeProvider();
        if (!provider.TryGetContentType(fileName, out var contentType))
        {
            contentType = "application/octet-stream";
        }

        return PhysicalFile(physicalPath, contentType, fileName);
    }

    private async Task LoadLessonContextAsync(int lessonId, CancellationToken cancellationToken)
    {
        Lesson = await _accessDbService.GetLessonByIdAsync(lessonId, cancellationToken);
        Progress = await _accessDbService.GetProgressByLessonIdAsync(lessonId, cancellationToken);
        CourseProgressPercent = 0;

        if (Lesson is null)
        {
            CoursePlaylist = [];
            CourseTitle = string.Empty;
            LessonNumber = null;
            return;
        }

        CoursePlaylist = await _accessDbService.GetLessonsByCourseIdAsync(Lesson.ID_cours, cancellationToken);
        var course = await _accessDbService.GetCourseByIdAsync(Lesson.ID_cours, cancellationToken);
        CourseTitle = course?.Course_name ?? string.Empty;
        if (string.IsNullOrWhiteSpace(CourseTitle))
        {
            var lastCourseCookie = HttpContext.Request.Cookies[LastCourseCookie];
            if (int.TryParse(lastCourseCookie, out var lastCourseId) && lastCourseId > 0)
            {
                var lastCourse = await _accessDbService.GetCourseByIdAsync(lastCourseId, cancellationToken);
                CourseTitle = lastCourse?.Course_name ?? string.Empty;
            }
        }
        LessonNumber = Lesson.number_lesson
            ?? CoursePlaylist.FirstOrDefault(x => x.ID_lesson == Lesson.ID_lesson)?.number_lesson
            ?? CoursePlaylist.Select((item, idx) => new { item.ID_lesson, Number = idx + 1 })
                .FirstOrDefault(x => x.ID_lesson == Lesson.ID_lesson)?.Number;
        SaveLastViewed(Lesson.ID_cours, Lesson.ID_lesson);

        var claimValue = HttpContext.User.FindFirstValue(ClaimTypes.NameIdentifier);
        if (int.TryParse(claimValue, out var userId) && userId > 0)
        {
            CourseProgressPercent = await _accessDbService.GetCourseProgressPercentForUserAsync(userId, Lesson.ID_cours, cancellationToken);
        }
    }

    private async Task LoadFirstAvailableLessonAsync(CancellationToken cancellationToken)
    {
        var courses = await _accessDbService.GetCoursesAsync(cancellationToken);
        foreach (var course in courses.OrderBy(c => c.ID_curs))
        {
            var lessons = await _accessDbService.GetLessonsByCourseIdAsync(course.ID_curs, cancellationToken);
            var firstLesson = lessons.FirstOrDefault();
            if (firstLesson is null)
            {
                continue;
            }

            await LoadLessonContextAsync(firstLesson.ID_lesson, cancellationToken);
            return;
        }
    }

    private async Task<int> ResolveLessonIdAsync(int? routeLessonId, int? routeCourseId, CancellationToken cancellationToken)
    {
        // Явный lessonId в URL всегда приоритетнее cookie: раньше при несовпадении ID курса в cookie
        // подставлялся «первый урок другого курса», из‑за этого ломались переходы с страницы курса.
        if (routeLessonId.HasValue && routeLessonId.Value > 0)
        {
            var direct = await _accessDbService.GetLessonByIdAsync(routeLessonId.Value, cancellationToken);
            if (direct is not null)
            {
                return routeLessonId.Value;
            }
        }

        // Открытие уроков по выбранному курсу (главная, карточка курса): /Lesson?courseId=...
        if (routeCourseId.HasValue && routeCourseId.Value > 0)
        {
            var lessons = await _accessDbService.GetLessonsByCourseIdAsync(routeCourseId.Value, cancellationToken);
            var firstLessonId = lessons.FirstOrDefault()?.ID_lesson;
            if (firstLessonId.HasValue && firstLessonId.Value > 0)
            {
                return firstLessonId.Value;
            }

            // У курса нет уроков в БД — не подставлять урок из другого курса (cookie / 201).
            return -1;
        }

        var courseCookie = HttpContext.Request.Cookies[LastCourseCookie];
        var hasLastCourse = int.TryParse(courseCookie, out var lastCourseId) && lastCourseId > 0;

        if (hasLastCourse)
        {
            var lessons = await _accessDbService.GetLessonsByCourseIdAsync(lastCourseId, cancellationToken);
            var firstLessonId = lessons.FirstOrDefault()?.ID_lesson;
            if (firstLessonId.HasValue && firstLessonId.Value > 0)
            {
                return firstLessonId.Value;
            }
        }

        if (int.TryParse(HttpContext.Request.Cookies[LastLessonCookie], out var cookieLessonId) && cookieLessonId > 0)
        {
            var fromCookie = await _accessDbService.GetLessonByIdAsync(cookieLessonId, cancellationToken);
            if (fromCookie is not null)
            {
                return cookieLessonId;
            }
        }

        // Без фиксированного 201: первый урок в каталоге (как «последний выбранный» контекст отсутствует).
        return await GetFirstLessonIdInCatalogAsync(cancellationToken);
    }

    private async Task<int> GetFirstLessonIdInCatalogAsync(CancellationToken cancellationToken)
    {
        var courses = await _accessDbService.GetCoursesAsync(cancellationToken);
        foreach (var course in courses.OrderBy(c => c.ID_curs))
        {
            var lessons = await _accessDbService.GetLessonsByCourseIdAsync(course.ID_curs, cancellationToken);
            var first = lessons.FirstOrDefault()?.ID_lesson;
            if (first is > 0)
            {
                return first.Value;
            }
        }

        return 0;
    }

    private void SaveLastViewed(int courseId, int lessonId)
    {
        var options = new CookieOptions
        {
            Expires = DateTimeOffset.UtcNow.AddDays(30),
            IsEssential = true
        };
        HttpContext.Response.Cookies.Append(LastCourseCookie, courseId.ToString(), options);
        HttpContext.Response.Cookies.Append(LastLessonCookie, lessonId.ToString(), options);
    }
}
