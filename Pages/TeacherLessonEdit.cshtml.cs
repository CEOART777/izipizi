using System.ComponentModel.DataAnnotations;
using System.Security.Claims;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize(Roles = "teacher")]
public class TeacherLessonEditModel : PageModel
{
    private readonly AccessDbService _db;
    private readonly ILogger<TeacherLessonEditModel> _logger;

    public TeacherLessonEditModel(AccessDbService db, ILogger<TeacherLessonEditModel> logger)
    {
        _db = db;
        _logger = logger;
    }

    [BindProperty(SupportsGet = true)]
    public int LessonId { get; set; }

    public int CourseId { get; private set; }
    public CourseLessonItem? Lesson { get; private set; }

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

    public async Task<IActionResult> OnGetAsync(CancellationToken cancellationToken)
    {
        var ok = await LoadAndCheckAsync(cancellationToken);
        return ok ? Page() : Forbid();
    }

    public async Task<IActionResult> OnPostAsync(int lessonId, CancellationToken cancellationToken)
    {
        LessonId = lessonId;
        var okAccess = await LoadAndCheckAsync(cancellationToken);
        if (!okAccess)
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
            var ok = await _db.UpdateLessonAsync(
                LessonId,
                CourseId,
                Input.NumberLesson,
                Input.Name.Trim(),
                (Input.VideoUrl ?? string.Empty).Trim(),
                (Input.MaterialsUrl ?? string.Empty).Trim(),
                (Input.LessonDescription ?? string.Empty).Trim(),
                cancellationToken);
            SuccessMessage = ok ? "Сохранено." : "Не удалось сохранить изменения.";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to update lesson as teacher.");
            ErrorMessage = "Ошибка при сохранении в базе.";
        }

        await LoadAndCheckAsync(cancellationToken);
        return Page();
    }

    public async Task<IActionResult> OnPostDeleteAsync(int lessonId, CancellationToken cancellationToken)
    {
        LessonId = lessonId;
        var okAccess = await LoadAndCheckAsync(cancellationToken);
        if (!okAccess)
        {
            return Forbid();
        }

        try
        {
            var deleted = await _db.DeleteLessonAsync(LessonId, cancellationToken);
            if (!deleted)
            {
                ErrorMessage = "Не удалось удалить урок.";
                return Page();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete lesson as teacher.");
            ErrorMessage = "Ошибка при удалении урока.";
            return Page();
        }

        return RedirectToPage("/TeacherCourse", new { courseId = CourseId, show = "all" });
    }

    private async Task<bool> LoadAndCheckAsync(CancellationToken cancellationToken)
    {
        if (LessonId <= 0)
        {
            ErrorMessage = "Некорректный урок.";
            Lesson = null;
            return false;
        }

        var teacherIdValue = HttpContext.User.FindFirstValue(ClaimTypes.NameIdentifier);
        if (!int.TryParse(teacherIdValue, out var teacherId) || teacherId <= 0)
        {
            return false;
        }

        Lesson = await _db.GetLessonByIdAsync(LessonId, cancellationToken);
        if (Lesson is null)
        {
            return false;
        }

        CourseId = Lesson.ID_cours;
        var assignedCourseIds = await _db.GetAssignedCourseIdsForTeacherAsync(teacherId, cancellationToken);
        if (Lesson.ID_cours <= 0 || !assignedCourseIds.Contains(Lesson.ID_cours))
        {
            return false;
        }

        Input = new InputModel
        {
            Name = Lesson.Cours_name,
            NumberLesson = Lesson.number_lesson,
            VideoUrl = Lesson.video_url,
            MaterialsUrl = Lesson.meterials_url,
            LessonDescription = Lesson.course_discription
        };

        return true;
    }
}

