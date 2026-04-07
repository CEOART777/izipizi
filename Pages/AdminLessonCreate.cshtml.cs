using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;

namespace практика_2._0.Pages;

[Authorize(Roles = "admin")]
public class AdminLessonCreateModel : PageModel
{
    private readonly AccessDbService _db;
    private readonly ILogger<AdminLessonCreateModel> _logger;

    public AdminLessonCreateModel(AccessDbService db, ILogger<AdminLessonCreateModel> logger)
    {
        _db = db;
        _logger = logger;
    }

    public int CourseId { get; private set; }
    public string CourseName { get; private set; } = string.Empty;

    [BindProperty]
    public InputModel Input { get; set; } = new();

    public string? ErrorMessage { get; private set; }
    public string? SuccessMessage { get; private set; }

    public sealed class InputModel
    {
        [Required]
        public string Name { get; set; } = string.Empty;

        public int? NumberLesson { get; set; }

        public string VideoUrl { get; set; } = string.Empty;

        public string MaterialsUrl { get; set; } = string.Empty;

        public string? LessonDescription { get; set; }
    }

    public async Task OnGetAsync(int courseId, CancellationToken cancellationToken)
    {
        CourseId = courseId;
        var c = await _db.GetCourseByIdAsync(courseId, cancellationToken);
        CourseName = c?.Course_name ?? $"Курс #{courseId}";
    }

    public async Task<IActionResult> OnPostAsync(int courseId, CancellationToken cancellationToken)
    {
        CourseId = courseId;
        var c = await _db.GetCourseByIdAsync(courseId, cancellationToken);
        CourseName = c?.Course_name ?? $"Курс #{courseId}";

        if (!ModelState.IsValid)
        {
            ErrorMessage = "Проверьте название урока.";
            return Page();
        }

        try
        {
            var newLessonId = await _db.CreateLessonAdminAsync(
                courseId,
                Input.NumberLesson,
                Input.Name.Trim(),
                (Input.VideoUrl ?? string.Empty).Trim(),
                (Input.MaterialsUrl ?? string.Empty).Trim(),
                (Input.LessonDescription ?? string.Empty).Trim(),
                cancellationToken);

            SuccessMessage = $"Урок добавлен (ID: {newLessonId}).";
            return RedirectToPage("/Lesson", new { lessonId = newLessonId });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create lesson in admin.");
            ErrorMessage = "Ошибка при добавлении урока в базу.";
            return Page();
        }
    }
}

