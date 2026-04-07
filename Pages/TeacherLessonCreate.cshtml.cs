using System.ComponentModel.DataAnnotations;
using System.Security.Claims;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;

namespace практика_2._0.Pages;

[Authorize(Roles = "teacher")]
public class TeacherLessonCreateModel : PageModel
{
    private readonly AccessDbService _db;
    private readonly ILogger<TeacherLessonCreateModel> _logger;

    public TeacherLessonCreateModel(AccessDbService db, ILogger<TeacherLessonCreateModel> logger)
    {
        _db = db;
        _logger = logger;
    }

    [BindProperty(SupportsGet = true)]
    public int CourseId { get; set; }
    public string CourseName { get; private set; } = string.Empty;

    [BindProperty]
    public InputModel Input { get; set; } = new();

    public string? ErrorMessage { get; private set; }

    public sealed class InputModel
    {
        [Required]
        public string Name { get; set; } = string.Empty;
        public int? NumberLesson { get; set; }
        public string VideoUrl { get; set; } = string.Empty;
        public string MaterialsUrl { get; set; } = string.Empty;
        public string? LessonDescription { get; set; }
    }

    public async Task<IActionResult> OnGetAsync(CancellationToken cancellationToken)
    {
        if (!await LoadAndCheckAsync(cancellationToken))
        {
            return Forbid();
        }
        return Page();
    }

    public async Task<IActionResult> OnPostAsync(CancellationToken cancellationToken)
    {
        if (!await LoadAndCheckAsync(cancellationToken))
        {
            return Forbid();
        }

        if (!ModelState.IsValid)
        {
            ErrorMessage = "Проверьте поля урока.";
            return Page();
        }

        try
        {
            var lessonId = await _db.CreateLessonAdminAsync(
                CourseId,
                Input.NumberLesson,
                Input.Name.Trim(),
                (Input.VideoUrl ?? string.Empty).Trim(),
                (Input.MaterialsUrl ?? string.Empty).Trim(),
                (Input.LessonDescription ?? string.Empty).Trim(),
                cancellationToken);

            return RedirectToPage("/TeacherCourse", new { courseId = CourseId, show = "all" });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create lesson as teacher.");
            ErrorMessage = "Не удалось добавить урок.";
            return Page();
        }
    }

    private async Task<bool> LoadAndCheckAsync(CancellationToken cancellationToken)
    {
        if (CourseId <= 0)
        {
            return false;
        }

        var teacherIdValue = HttpContext.User.FindFirstValue(ClaimTypes.NameIdentifier);
        if (!int.TryParse(teacherIdValue, out var teacherId) || teacherId <= 0)
        {
            return false;
        }

        var course = await _db.GetCourseByIdAsync(CourseId, cancellationToken);
        if (course is null)
        {
            return false;
        }

        var assignedIds = await _db.GetAssignedCourseIdsForTeacherAsync(teacherId, cancellationToken);
        if (!(course.teacher_user_id == teacherId || assignedIds.Contains(CourseId)))
        {
            return false;
        }

        CourseName = course.Course_name;
        return true;
    }
}
