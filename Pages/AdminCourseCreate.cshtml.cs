using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize(Roles = "admin")]
public class AdminCourseCreateModel : PageModel
{
    private readonly AccessDbService _db;
    private readonly ILogger<AdminCourseCreateModel> _logger;

    public AdminCourseCreateModel(AccessDbService db, ILogger<AdminCourseCreateModel> logger)
    {
        _db = db;
        _logger = logger;
    }

    [BindProperty]
    public InputModel Input { get; set; } = new();
    public IReadOnlyList<CategoryItem> Categories { get; private set; } = [];
    public IReadOnlyList<UserItem> Teachers { get; private set; } = [];

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

    public async Task OnGetAsync(CancellationToken cancellationToken)
    {
        Categories = await _db.GetCategoriesAsync(cancellationToken);
        Teachers = (await _db.GetAllUsersAsync(cancellationToken))
            .Where(u => string.Equals((u.role ?? string.Empty).Trim(), "teacher", StringComparison.OrdinalIgnoreCase))
            .OrderBy(u => u.full_name)
            .ToList();
    }

    public async Task<IActionResult> OnPostAsync(CancellationToken cancellationToken)
    {
        if (!ModelState.IsValid)
        {
            ErrorMessage = "Проверьте поля курса.";
            Categories = await _db.GetCategoriesAsync(cancellationToken);
            Teachers = (await _db.GetAllUsersAsync(cancellationToken))
                .Where(u => string.Equals((u.role ?? string.Empty).Trim(), "teacher", StringComparison.OrdinalIgnoreCase))
                .OrderBy(u => u.full_name)
                .ToList();
            return Page();
        }

        try
        {
            var newId = await _db.CreateCourseAdminAsync(
                0,
                Input.CategoryId,
                Input.Price,
                Input.Name.Trim(),
                Input.Rating,
                (Input.PreviewUrl ?? string.Empty).Trim(),
                Input.TeacherUserId,
                cancellationToken);

            if (Input.TeacherUserId is int teacherId && teacherId > 0)
            {
                _ = await _db.AssignTeacherToCourseAsync(teacherId, newId, cancellationToken);
            }

            SuccessMessage = $"Курс добавлен (ID: {newId}).";
            return RedirectToPage("/AdminCourseEdit", new { courseId = newId });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create course in admin.");
            ErrorMessage = "Ошибка при добавлении курса в базу.";
            Categories = await _db.GetCategoriesAsync(cancellationToken);
            Teachers = (await _db.GetAllUsersAsync(cancellationToken))
                .Where(u => string.Equals((u.role ?? string.Empty).Trim(), "teacher", StringComparison.OrdinalIgnoreCase))
                .OrderBy(u => u.full_name)
                .ToList();
            return Page();
        }
    }
}

