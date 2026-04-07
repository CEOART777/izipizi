using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize]
public class TasksModel : PageModel
{
    private readonly AccessDbService _accessDbService;

    public TasksModel(AccessDbService accessDbService)
    {
        _accessDbService = accessDbService;
    }

    [BindProperty(SupportsGet = true)]
    public int? LessonId { get; set; }

    /// <summary>True, если передан LessonId, но в БД нет ДЗ именно по этому уроку (показываем полный список).</summary>
    public bool NoHomeworkForSelectedLesson { get; private set; }

    public IReadOnlyList<HomeworkReviewItem> Homework { get; private set; } = [];

    public async Task OnGetAsync(CancellationToken cancellationToken)
    {
        var userId = await ResolveCurrentUserIdAsync(cancellationToken);
        if (userId is null)
        {
            return;
        }

        var all = await _accessDbService.GetHomeworkReviewsByUserIdAsync(userId.Value, cancellationToken);
        if (LessonId.HasValue && LessonId.Value > 0)
        {
            var filtered = all.Where(h => h.LessonId == LessonId.Value).ToList();
            if (filtered.Count > 0)
            {
                Homework = filtered;
            }
            else
            {
                NoHomeworkForSelectedLesson = true;
                Homework = all;
            }
        }
        else
        {
            Homework = all;
        }
    }

    private async Task<int?> ResolveCurrentUserIdAsync(CancellationToken cancellationToken)
    {
        var email = HttpContext.User.FindFirstValue(ClaimTypes.Email);
        if (!string.IsNullOrWhiteSpace(email))
        {
            var userByEmail = await _accessDbService.GetUserByEmailAsync(email.Trim().Trim('"'), cancellationToken);
            if (userByEmail is not null)
            {
                return userByEmail.ID_user;
            }
        }

        var claimValue = HttpContext.User.FindFirstValue(ClaimTypes.NameIdentifier);
        if (int.TryParse(claimValue, out var claimUserId) && claimUserId > 0)
        {
            var byId = await _accessDbService.GetUserByIdAsync(claimUserId, cancellationToken);
            var emailClaim = HttpContext.User.FindFirstValue(ClaimTypes.Email);
            var normEmailClaim = string.IsNullOrWhiteSpace(emailClaim) ? null : emailClaim.Trim().Trim('"').ToLowerInvariant();
            var normByIdEmail = string.IsNullOrWhiteSpace(byId?.email) ? null : byId.email.Trim().Trim('"').ToLowerInvariant();
            if (byId is not null && (string.IsNullOrWhiteSpace(emailClaim) ||
                                    string.Equals(normByIdEmail, normEmailClaim, StringComparison.OrdinalIgnoreCase)))
            {
                return claimUserId;
            }
        }

        return null;
    }
}
