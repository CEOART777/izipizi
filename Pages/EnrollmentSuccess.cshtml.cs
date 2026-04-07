using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize]
public class EnrollmentSuccessModel : PageModel
{
    private readonly AccessDbService _accessDbService;

    public EnrollmentSuccessModel(AccessDbService accessDbService)
    {
        _accessDbService = accessDbService;
    }

    public CourseItem? Course { get; private set; }

    public async Task OnGetAsync(int? courseId, CancellationToken cancellationToken)
    {
        if (courseId is null)
        {
            return;
        }

        var userId = await ResolveCurrentUserIdAsync(cancellationToken);
        if (userId is null)
        {
            return;
        }

        var enrollment = await _accessDbService.GetEnrollmentByUserAndCourseAsync(userId.Value, courseId.Value, cancellationToken);
        if (enrollment is null)
        {
            return;
        }

        Course = await _accessDbService.GetCourseByIdAsync(courseId.Value, cancellationToken);
    }

    private async Task<int?> ResolveCurrentUserIdAsync(CancellationToken cancellationToken)
    {
        var claimValue = HttpContext.User.FindFirstValue(ClaimTypes.NameIdentifier);
        if (int.TryParse(claimValue, out var claimUserId))
        {
            return claimUserId;
        }

        var email = HttpContext.User.FindFirstValue(ClaimTypes.Email);
        if (!string.IsNullOrWhiteSpace(email))
        {
            var userByEmail = await _accessDbService.GetUserByEmailAsync(email, cancellationToken);
            if (userByEmail is not null)
            {
                return userByEmail.ID_user;
            }
        }

        return null;
    }
}
