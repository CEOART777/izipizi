using System.Security.Claims;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize(Roles = "teacher")]
public class TeacherModel : PageModel
{
    private readonly AccessDbService _db;
    private readonly ILogger<TeacherModel> _logger;

    public TeacherModel(AccessDbService db, ILogger<TeacherModel> logger)
    {
        _db = db;
        _logger = logger;
    }

    public IReadOnlyList<CourseItem> Courses { get; private set; } = [];
    public string? ErrorMessage { get; private set; }

    public async Task OnGetAsync(CancellationToken cancellationToken)
    {
        var teacherIdValue = HttpContext.User.FindFirstValue(ClaimTypes.NameIdentifier);
        if (!int.TryParse(teacherIdValue, out var teacherId) || teacherId <= 0)
        {
            ErrorMessage = "Не удалось определить учителя.";
            return;
        }

        try
        {
            var list = (await _db.GetCoursesByTeacherIdAsync(teacherId, cancellationToken)).ToList();
            Courses = list;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load teacher courses.");
            ErrorMessage = "Не удалось загрузить курсы учителя.";
            Courses = [];
        }
    }
}

