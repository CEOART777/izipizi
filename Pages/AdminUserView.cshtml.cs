using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize(Roles = "admin")]
public class AdminUserViewModel : PageModel
{
    private readonly AccessDbService _db;
    private readonly ILogger<AdminUserViewModel> _logger;

    public AdminUserViewModel(AccessDbService db, ILogger<AdminUserViewModel> logger)
    {
        _db = db;
        _logger = logger;
    }

    public UserItem? User { get; private set; }
    public IReadOnlyList<EnrollmentItem> Enrollments { get; private set; } = [];
    public IReadOnlyDictionary<int, CourseItem> CourseMap { get; private set; } = new Dictionary<int, CourseItem>();
    public string? ErrorMessage { get; private set; }

    public async Task OnGetAsync(int userId, CancellationToken cancellationToken)
    {
        if (userId <= 0)
        {
            ErrorMessage = "Некорректный пользователь.";
            return;
        }

        try
        {
            User = await _db.GetUserByIdAsync(userId, cancellationToken);
            if (User is null)
            {
                ErrorMessage = "Пользователь не найден.";
                return;
            }

            Enrollments = (await _db.GetEnrollmentsByUserIdAsync(userId, cancellationToken))
                .OrderByDescending(e => e.ID_enrolment)
                .ToList();

            var map = new Dictionary<int, CourseItem>();
            foreach (var e in Enrollments)
            {
                if (e.ID_cours <= 0 || map.ContainsKey(e.ID_cours)) continue;
                var c = await _db.GetCourseByIdAsync(e.ID_cours, cancellationToken);
                if (c is not null)
                {
                    map[e.ID_cours] = c;
                }
            }
            CourseMap = map;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load admin user view.");
            ErrorMessage = "Не удалось загрузить данные пользователя.";
            User = null;
            Enrollments = [];
            CourseMap = new Dictionary<int, CourseItem>();
        }
    }

    public IActionResult OnPostBack() => RedirectToPage("/AdminUsers");
}

