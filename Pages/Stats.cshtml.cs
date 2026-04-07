using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize]
public class StatsModel : PageModel
{
    private readonly AccessDbService _accessDbService;

    public StatsModel(AccessDbService accessDbService)
    {
        _accessDbService = accessDbService;
    }

    public int LessonsPassed { get; private set; }
    public int HomeworkCount { get; private set; }
    public decimal AvgProgress { get; private set; }
    public int PurchasedCoursesCount { get; private set; }
    public int ActiveEnrollmentsCount { get; private set; }

    public async Task OnGetAsync(int? userId, CancellationToken cancellationToken)
    {
        var resolvedUserId = await ResolveCurrentUserIdAsync(userId, cancellationToken);
        if (resolvedUserId is null)
        {
            return;
        }

        var homework = await _accessDbService.GetHomeworkReviewsByUserIdAsync(resolvedUserId.Value, cancellationToken);
        var purchased = await _accessDbService.GetPurchasedCoursesByUserIdAsync(resolvedUserId.Value, cancellationToken);
        var enrollments = await _accessDbService.GetEnrollmentsByUserIdAsync(resolvedUserId.Value, cancellationToken);

        // Синхронизируем статистику с профилем: считаем только актуальную запись на курс.
        var latestPerCourse = enrollments
            .GroupBy(e => e.ID_cours)
            .Select(g => g.OrderByDescending(x => x.ID_enrolment).First())
            .ToList();

        var finishedEnrollments = latestPerCourse
            .Where(e =>
            {
                var st = (e.status ?? string.Empty).Trim().ToLowerInvariant();
                return st is "finished" or "complete";
            })
            .ToList();
        var activeEnrollments = latestPerCourse
            .Where(e =>
            {
                var st = (e.status ?? string.Empty).Trim().ToLowerInvariant();
                return st != "cancelled" && st != "finished" && st != "refund_requested" && st != "complete";
            })
            .ToList();

        HomeworkCount = homework.Count;
        LessonsPassed = finishedEnrollments.Count;
        AvgProgress = 0;
        PurchasedCoursesCount = purchased.Count;
        ActiveEnrollmentsCount = activeEnrollments.Count;
    }

    private async Task<int?> ResolveCurrentUserIdAsync(int? routeUserId, CancellationToken cancellationToken)
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
