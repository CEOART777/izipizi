using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize(Roles = "admin")]
public class AdminCourseEditModel : PageModel
{
    private readonly AccessDbService _db;
    private readonly ILogger<AdminCourseEditModel> _logger;

    public AdminCourseEditModel(AccessDbService db, ILogger<AdminCourseEditModel> logger)
    {
        _db = db;
        _logger = logger;
    }

    public CourseItem? Course { get; private set; }
    public IReadOnlyList<CategoryItem> Categories { get; private set; } = [];
    public IReadOnlyList<UserItem> Teachers { get; private set; } = [];

    [BindProperty]
    public InputModel Input { get; set; } = new();

    public string? ErrorMessage { get; private set; }
    public string? SuccessMessage { get; private set; }

    public sealed class InputModel
    {
        [Required]
        public string Name { get; set; } = string.Empty;

        [Range(0, 1_000_000)]
        public decimal Price { get; set; }

        [Range(1, 1_000_000)]
        public int CategoryId { get; set; }

        [Range(0, 5)]
        public decimal Rating { get; set; }

        public string PreviewUrl { get; set; } = string.Empty;

        public int? TeacherUserId { get; set; }
    }

    public async Task OnGetAsync(int courseId, CancellationToken cancellationToken)
    {
        await LoadAsync(courseId, cancellationToken);
    }

    public async Task<IActionResult> OnPostAsync(int courseId, CancellationToken cancellationToken)
    {
        if (!ModelState.IsValid)
        {
            ErrorMessage = "Проверьте корректность полей курса.";
            await LoadAsync(courseId, cancellationToken);
            return Page();
        }

        try
        {
            var currentCourse = await _db.GetCourseByIdAsync(courseId, cancellationToken);
            if (currentCourse is null)
            {
                ErrorMessage = "Курс не найден.";
                await LoadAsync(courseId, cancellationToken);
                return Page();
            }

            var ok = await _db.UpdateCourseAdminAsync(
                courseId,
                currentCourse.ID_lesson,
                Input.CategoryId,
                Input.Price,
                Input.Name.Trim(),
                Input.Rating,
                (Input.PreviewUrl ?? string.Empty).Trim(),
                Input.TeacherUserId,
                cancellationToken);

            if (ok && Input.TeacherUserId is int teacherId && teacherId > 0)
            {
                _ = await _db.AssignTeacherToCourseAsync(teacherId, courseId, cancellationToken);
            }

            SuccessMessage = ok ? "Сохранено." : "Не удалось сохранить изменения.";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to update course in admin.");
            ErrorMessage = "Ошибка при сохранении в базе.";
        }

        await LoadAsync(courseId, cancellationToken);
        return Page();
    }

    private async Task LoadAsync(int courseId, CancellationToken cancellationToken)
    {
        Course = await _db.GetCourseByIdAsync(courseId, cancellationToken);
        Categories = await _db.GetCategoriesAsync(cancellationToken);
        Teachers = (await _db.GetAllUsersAsync(cancellationToken))
            .Where(u => string.Equals((u.role ?? string.Empty).Trim(), "teacher", StringComparison.OrdinalIgnoreCase))
            .OrderBy(u => u.full_name)
            .ToList();
        if (Course is null)
        {
            return;
        }

        Input = new InputModel
        {
            Name = Course.Course_name,
            Price = Course.price,
            CategoryId = Course.ID_categorise,
            Rating = Course.rating,
            PreviewUrl = Course.preview_url,
            TeacherUserId = Course.teacher_user_id
        };
    }
}

