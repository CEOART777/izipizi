using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.AspNetCore.Mvc;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize(Roles = "admin")]
public class AdminCoursesModel : PageModel
{
    private readonly AccessDbService _db;
    private readonly ILogger<AdminCoursesModel> _logger;

    public AdminCoursesModel(AccessDbService db, ILogger<AdminCoursesModel> logger)
    {
        _db = db;
        _logger = logger;
    }

    public IReadOnlyList<CourseItem> Courses { get; private set; } = [];
    public IReadOnlyDictionary<int, string> CategoryNames { get; private set; } = new Dictionary<int, string>();
    public string? ErrorMessage { get; private set; }

    public async Task OnGetAsync(CancellationToken cancellationToken)
    {
        try
        {
            // В списке показываем как есть (rating тут fallback), средние по отзывам считаются отдельно на витрине.
            Courses = await _db.GetCoursesAsync(cancellationToken);
            CategoryNames = (await _db.GetCategoriesAsync(cancellationToken))
                .GroupBy(c => c.ID_categorise)
                .ToDictionary(g => g.Key, g => g.First().name_categori);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load courses for admin.");
            Courses = [];
            CategoryNames = new Dictionary<int, string>();
        }
    }

    public async Task<IActionResult> OnPostDeleteAsync(int courseId, CancellationToken cancellationToken)
    {
        try
        {
            await _db.DeleteCourseAdminAsync(courseId, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete course in admin.");
            ErrorMessage = "Не удалось удалить курс. Возможно, есть связанные записи (уроки/записи/оплаты/отзывы).";
        }

        await OnGetAsync(cancellationToken);
        return Page();
    }
}

