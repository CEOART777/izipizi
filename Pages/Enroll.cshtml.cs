using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize]
public class EnrollModel : PageModel
{
    private readonly AccessDbService _accessDbService;
    private readonly ILogger<EnrollModel> _logger;

    public EnrollModel(AccessDbService accessDbService, ILogger<EnrollModel> logger)
    {
        _accessDbService = accessDbService;
        _logger = logger;
    }

    public CourseItem? Course { get; private set; }
    public UserItem? User { get; private set; }
    public EnrollmentItem? ExistingEnrollment { get; private set; }

    public async Task OnGetAsync(int? courseId, int? userId, CancellationToken cancellationToken)
    {
        if (HttpContext.User.IsInRole("teacher"))
        {
            // Учитель не записывается на курсы
            Course = null;
            User = null;
            ExistingEnrollment = null;
            return;
        }

        var currentCourseId = courseId ?? 101;
        var currentUserId = await ResolveCurrentUserIdAsync(userId, cancellationToken);
        if (currentUserId is null)
        {
            return;
        }

        try
        {
            Course = await _accessDbService.GetCourseByIdAsync(currentCourseId, cancellationToken);
            User = await _accessDbService.GetUserByIdAsync(currentUserId.Value, cancellationToken);
            ExistingEnrollment = await _accessDbService.GetEnrollmentByUserAndCourseAsync(currentUserId.Value, currentCourseId, cancellationToken);
            var st = (ExistingEnrollment?.status ?? string.Empty).Trim();
            if (string.Equals(st, "cancelled", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(st, "refunded", StringComparison.OrdinalIgnoreCase))
            {
                ExistingEnrollment = null; // отменённые/возвращённые не должны блокировать повторную запись
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load enroll data.");
            Course = null;
            User = null;
            ExistingEnrollment = null;
        }
    }

    public async Task<IActionResult> OnPostAsync(int courseId, CancellationToken cancellationToken)
    {
        if (HttpContext.User.IsInRole("teacher"))
        {
            return RedirectToPage("/Course", new { courseId });
        }

        var userId = await ResolveCurrentUserIdAsync(null, cancellationToken);
        if (userId is null)
        {
            return RedirectToPage("/Login");
        }

        var course = await _accessDbService.GetCourseByIdAsync(courseId, cancellationToken);
        if (course is null)
        {
            return RedirectToPage("/Index");
        }

        return RedirectToPage("/Payment", new { courseId });
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
