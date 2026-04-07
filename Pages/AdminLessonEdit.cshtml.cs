using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize(Roles = "admin")]
public class AdminLessonEditModel : PageModel
{
    private readonly AccessDbService _db;
    private readonly ILogger<AdminLessonEditModel> _logger;

    public AdminLessonEditModel(AccessDbService db, ILogger<AdminLessonEditModel> logger)
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

        [Range(1, 1_000_000)]
        public int CourseId { get; set; }

        public string VideoUrl { get; set; } = string.Empty;

        public string MaterialsUrl { get; set; } = string.Empty;

        public string? LessonDescription { get; set; }
    }

    public async Task OnGetAsync(CancellationToken cancellationToken)
    {
        await LoadAsync(cancellationToken);
    }

    public async Task<IActionResult> OnPostAsync(int lessonId, CancellationToken cancellationToken)
    {
        LessonId = lessonId;
        if (!ModelState.IsValid)
        {
            ErrorMessage = "Проверьте поля урока.";
            await LoadAsync(cancellationToken);
            return Page();
        }

        try
        {
            var ok = await _db.UpdateLessonAsync(
                LessonId,
                Input.CourseId,
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
            _logger.LogError(ex, "Failed to update lesson in admin.");
            ErrorMessage = "Ошибка при сохранении в базе.";
        }

        await LoadAsync(cancellationToken);
        return Page();
    }

    private async Task LoadAsync(CancellationToken cancellationToken)
    {
        if (LessonId <= 0)
        {
            ErrorMessage = "Некорректный урок.";
            Lesson = null;
            return;
        }

        Lesson = await _db.GetLessonByIdAsync(LessonId, cancellationToken);
        if (Lesson is null)
        {
            return;
        }

        CourseId = Lesson.ID_cours;
        Input = new InputModel
        {
            Name = Lesson.Cours_name,
            NumberLesson = Lesson.number_lesson,
            CourseId = Lesson.ID_cours,
            VideoUrl = Lesson.video_url,
            MaterialsUrl = Lesson.meterials_url,
            LessonDescription = Lesson.course_discription
        };
    }
}

