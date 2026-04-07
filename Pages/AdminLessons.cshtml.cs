using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize(Roles = "admin")]
public class AdminLessonsModel : PageModel
{
    private readonly AccessDbService _db;
    private readonly ILogger<AdminLessonsModel> _logger;

    public AdminLessonsModel(AccessDbService db, ILogger<AdminLessonsModel> logger)
    {
        _db = db;
        _logger = logger;
    }

    [BindProperty(SupportsGet = true)]
    public int CourseId { get; set; }

    public string CourseName { get; private set; } = string.Empty;
    public IReadOnlyList<CourseLessonItem> Lessons { get; private set; } = [];
    public string? ErrorMessage { get; private set; }

    public async Task OnGetAsync(CancellationToken cancellationToken)
    {
        if (CourseId <= 0)
        {
            ErrorMessage = "Некорректный курс.";
            return;
        }

        try
        {
            var c = await _db.GetCourseByIdAsync(CourseId, cancellationToken);
            CourseName = c?.Course_name ?? $"Курс #{CourseId}";
            Lessons = await _db.GetLessonsByCourseIdAsync(CourseId, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load lessons for admin.");
            ErrorMessage = "Не удалось загрузить уроки.";
            Lessons = [];
        }
    }
}

