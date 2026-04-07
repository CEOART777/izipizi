using System.ComponentModel.DataAnnotations;
using System.Security.Claims;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize]
public class ProfileSecurityModel : PageModel
{
    private readonly AccessDbService _accessDbService;

    public ProfileSecurityModel(AccessDbService accessDbService)
    {
        _accessDbService = accessDbService;
    }

    public UserItem? User { get; private set; }
    public string? SuccessMessage { get; private set; }
    public string? ErrorMessage { get; private set; }

    [BindProperty]
    public PasswordInput Input { get; set; } = new();

    public sealed class PasswordInput
    {
        [Required]
        public string CurrentPassword { get; set; } = string.Empty;

        [Required]
        [MinLength(4)]
        public string NewPassword { get; set; } = string.Empty;

        [Required]
        public string ConfirmPassword { get; set; } = string.Empty;
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

        if (!ModelState.IsValid)
        {
            ErrorMessage = "Проверьте корректность заполнения полей.";
            return Page();
        }

        if (!string.Equals(Input.NewPassword, Input.ConfirmPassword, StringComparison.Ordinal))
        {
            ErrorMessage = "Новый пароль и подтверждение не совпадают.";
            return Page();
        }

        var currentEmail = HttpContext.User.FindFirstValue(ClaimTypes.Email);
        var changed = await _accessDbService.ChangeUserPasswordAsync(id.Value, Input.CurrentPassword, Input.NewPassword, currentEmail, cancellationToken);
        if (!changed)
        {
            ErrorMessage = "Текущий пароль указан неверно.";
            return Page();
        }

        SuccessMessage = "Пароль успешно изменен.";
        Input = new PasswordInput();
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
