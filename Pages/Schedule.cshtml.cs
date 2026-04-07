using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize]
public class ScheduleModel : PageModel
{
    private const string LastCourseCookie = "last_course_id";
    private const string LastLessonCookie = "last_lesson_id";
    private readonly AccessDbService _accessDbService;

    public ScheduleModel(AccessDbService accessDbService)
    {
        _accessDbService = accessDbService;
    }

    public IReadOnlyList<EnrollmentItem> Enrollments { get; private set; } = [];
    public IReadOnlyList<CourseItem> Courses { get; private set; } = [];
    public IReadOnlyList<CourseItem> PurchasedCourses { get; private set; } = [];
    public IReadOnlyDictionary<int, EnrollmentItem> EnrollmentByCourseId { get; private set; } = new Dictionary<int, EnrollmentItem>();
    public int? LastViewedCourseId { get; private set; }
    public int? LastViewedLessonId { get; private set; }

    public async Task OnGetAsync(int? userId, CancellationToken cancellationToken)
    {
        var resolvedUserId = await ResolveCurrentUserIdAsync(userId, cancellationToken);
        if (resolvedUserId is null)
        {
            return;
        }

        var enrollmentsAll = await _accessDbService.GetEnrollmentsByUserIdAsync(resolvedUserId.Value, cancellationToken);
        var latestEnrollments = enrollmentsAll
            .GroupBy(e => e.ID_cours)
            .Select(g => g.OrderByDescending(x => x.ID_enrolment).First())
            .ToList();
        // cancelled — полностью "отписан": не показываем его в расписании и не возвращаем через PAY fallback.
        Enrollments = latestEnrollments
            .Where(e =>
            {
                var st = (e.status ?? string.Empty).Trim().ToLowerInvariant();
                // refund_requested показываем, но как "ожидает отмены"
                return st != "cancelled";
            })
            .ToList();

        EnrollmentByCourseId = Enrollments
            .GroupBy(e => e.ID_cours)
            .ToDictionary(g => g.Key, g => g.First());

        var purchasedAll = await _accessDbService.GetPurchasedCoursesByUserIdAsync(resolvedUserId.Value, cancellationToken);
        var cancelledCourseIds = enrollmentsAll
            .GroupBy(e => e.ID_cours)
            .Select(g => g.OrderByDescending(x => x.ID_enrolment).First())
            .Where(e =>
            {
                var st = (e.status ?? string.Empty).Trim().ToLowerInvariant();
                return st == "cancelled";
            })
            .Select(e => e.ID_cours)
            .Where(x => x > 0)
            .Distinct()
            .ToHashSet();
        PurchasedCourses = purchasedAll
            .Where(c => !cancelledCourseIds.Contains(c.ID_curs))
            .ToList();
        var courses = new List<CourseItem>();
        foreach (var enrollment in Enrollments)
        {
            var course = await _accessDbService.GetCourseByIdAsync(enrollment.ID_cours, cancellationToken);
            if (course is not null)
            {
                courses.Add(course);
            }
        }

        // Fallback: if ENROLLMENTS is sparse, still show courses paid by user.
        foreach (var purchased in PurchasedCourses)
        {
            if (courses.All(c => c.ID_curs != purchased.ID_curs))
            {
                courses.Add(purchased);
            }
        }

        Courses = courses;

        if (int.TryParse(HttpContext.Request.Cookies[LastCourseCookie], out var cid) && cid > 0)
        {
            LastViewedCourseId = cid;
        }
        if (int.TryParse(HttpContext.Request.Cookies[LastLessonCookie], out var lid) && lid > 0)
        {
            LastViewedLessonId = lid;
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
