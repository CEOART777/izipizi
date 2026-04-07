using System.ComponentModel.DataAnnotations;
using System.Security.Claims;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize]
public class ProfileNotificationsModel : PageModel
{
    private readonly AccessDbService _accessDbService;

    public ProfileNotificationsModel(AccessDbService accessDbService)
    {
        _accessDbService = accessDbService;
    }

    public UserItem? User { get; private set; }
    public string? SuccessMessage { get; private set; }

    [BindProperty]
    public NotificationInput Input { get; set; } = new();

    public sealed class NotificationInput
    {
        [Display(Name = "Email-уведомления")]
        public bool EmailNotifications { get; set; } = true;

        [Display(Name = "SMS-уведомления")]
        public bool SmsNotifications { get; set; } = true;

        [Display(Name = "Промо-рассылки")]
        public bool PromoNotifications { get; set; } = false;
    }

    public async Task OnGetAsync(int? userId, CancellationToken cancellationToken)
    {
        var id = await ResolveCurrentUserIdAsync(userId, cancellationToken);
        if (id is null)
        {
            return;
        }

        User = await _accessDbService.GetUserByIdAsync(id.Value, cancellationToken);
    }

    public async Task<IActionResult> OnPostAsync(int? userId, CancellationToken cancellationToken)
    {
        var id = await ResolveCurrentUserIdAsync(userId, cancellationToken);
        if (id is null)
        {
            return RedirectToPage("/Login");
        }

        User = await _accessDbService.GetUserByIdAsync(id.Value, cancellationToken);
        SuccessMessage = "Настройки уведомлений сохранены.";
        return Page();
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
        if (int.TryParse(claimValue, out var claimUserId))
        {
            return claimUserId;
        }

        return null;
    }
}
