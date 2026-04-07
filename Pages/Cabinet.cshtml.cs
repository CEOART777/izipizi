using System.Security.Claims;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize]
public class CabinetModel : PageModel
{
    private readonly AccessDbService _accessDbService;
    private readonly ILogger<CabinetModel> _logger;

    public CabinetModel(AccessDbService accessDbService, ILogger<CabinetModel> logger)
    {
        _accessDbService = accessDbService;
        _logger = logger;
    }

    public UserItem? User { get; private set; }
    public CourseItem? CurrentCourse { get; private set; }
    public LessonProgressItem? CurrentProgress { get; private set; }
    public IReadOnlyList<EnrollmentItem> Enrollments { get; private set; } = [];
    public IReadOnlyList<EnrollmentItem> ActiveEnrollments { get; private set; } = [];
    public IReadOnlyList<EnrollmentItem> FinishedEnrollments { get; private set; } = [];
    public int CourseProgressPercent { get; private set; }
    public int FinishedCount { get; private set; }
    public IReadOnlyDictionary<int, string> FinishedCourseNames { get; private set; } = new Dictionary<int, string>();

    public async Task OnGetAsync(int? userId, CancellationToken cancellationToken)
    {
        var id = await ResolveCurrentUserIdAsync(userId, cancellationToken);
        if (id is null)
        {
            return;
        }

        try
        {
            User = await _accessDbService.GetUserByIdAsync(id.Value, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load cabinet user.");
            User = null;
        }

        try
        {
            Enrollments = await _accessDbService.GetEnrollmentsByUserIdAsync(id.Value, cancellationToken);
            ActiveEnrollments = Enrollments
                .Where(e =>
                {
                    var st = (e.status ?? string.Empty).Trim().ToLowerInvariant();
                    return st != "cancelled" && st != "finished";
                })
                .ToList();
            FinishedEnrollments = Enrollments
                .Where(e => string.Equals((e.status ?? string.Empty).Trim(), "finished", StringComparison.OrdinalIgnoreCase))
                .ToList();
            FinishedCount = FinishedEnrollments.Count;

            var nameMap = new Dictionary<int, string>();
            foreach (var fe in FinishedEnrollments)
            {
                if (nameMap.ContainsKey(fe.ID_cours))
                {
                    continue;
                }

                var c = await _accessDbService.GetCourseByIdAsync(fe.ID_cours, cancellationToken);
                nameMap[fe.ID_cours] = c?.Course_name ?? $"Курс #{fe.ID_cours}";
            }

            FinishedCourseNames = nameMap;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load cabinet enrollments.");
            Enrollments = [];
            ActiveEnrollments = [];
            FinishedEnrollments = [];
            FinishedCount = 0;
            FinishedCourseNames = new Dictionary<int, string>();
        }

        try
        {
            if (id is not null && User?.ID_curs is int preferredCourseId && preferredCourseId > 0)
            {
                var match = await _accessDbService.GetEnrollmentByUserAndCourseAsync(id.Value, preferredCourseId, cancellationToken);
                var st = (match?.status ?? string.Empty).Trim().ToLowerInvariant();
                if (match is not null && st is not ("cancelled" or "finished"))
                {
                    CurrentCourse = await _accessDbService.GetCourseByIdAsync(preferredCourseId, cancellationToken);
                }
            }

            if (CurrentCourse is null && ActiveEnrollments.Count > 0)
            {
                CurrentCourse = await _accessDbService.GetCourseByIdAsync(ActiveEnrollments[0].ID_cours, cancellationToken);
            }

            if (CurrentCourse is not null)
            {
                CurrentProgress = await _accessDbService.GetProgressByLessonIdAsync(CurrentCourse.ID_lesson, cancellationToken);
                CourseProgressPercent = await _accessDbService.GetCourseProgressPercentForUserAsync(id.Value, CurrentCourse.ID_curs, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load cabinet course/progress.");
            CurrentCourse = null;
            CurrentProgress = null;
            CourseProgressPercent = 0;
        }
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
