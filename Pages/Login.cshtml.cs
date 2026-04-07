using System.ComponentModel.DataAnnotations;
using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;

namespace практика_2._0.Pages;

public class LoginModel : PageModel
{
    private readonly AccessDbService _accessDbService;
    private readonly ILogger<LoginModel> _logger;

    public LoginModel(AccessDbService accessDbService, ILogger<LoginModel> logger)
    {
        _accessDbService = accessDbService;
        _logger = logger;
    }

    [BindProperty]
    public LoginInput Input { get; set; } = new();

    [BindProperty(SupportsGet = true)]
    public string? ReturnUrl { get; set; }

    public string? ErrorMessage { get; private set; }

    public sealed class LoginInput
    {
        [Required]
        public string Email { get; set; } = string.Empty;

        [Required]
        public string Password { get; set; } = string.Empty;
    }

    public IActionResult OnGet()
    {
        if (User.Identity?.IsAuthenticated == true)
        {
            if (!string.IsNullOrWhiteSpace(ReturnUrl) && Url.IsLocalUrl(ReturnUrl))
            {
                return Redirect(ReturnUrl);
            }

            if (User.IsInRole("admin"))
            {
                return RedirectToPage("/Admin");
            }

            return RedirectToPage("/Cabinet");
        }

        return Page();
    }

    public async Task<IActionResult> OnPostAsync(CancellationToken cancellationToken)
    {
        if (!ModelState.IsValid)
        {
            ErrorMessage = "Введите корректные email и пароль.";
            return Page();
        }

        try
        {
            var user = await _accessDbService.GetUserByCredentialsAsync(
                Input.Email.Trim(),
                Input.Password,
                cancellationToken);

            if (user is null)
            {
                ErrorMessage = "Неверный email или пароль.";
                return Page();
            }

            // Авто‑разблокировка: если срок блокировки истёк — снимаем блок и пускаем в аккаунт.
            if (user.is_blocked == true && user.blocked_until is DateTime expiredUntil && expiredUntil <= DateTime.Now)
            {
                try
                {
                    await _accessDbService.SetUserBlockedAsync(user.ID_user, false, null, null, cancellationToken);
                    user = await _accessDbService.GetUserByIdAsync(user.ID_user, cancellationToken) ?? user;
                }
                catch
                {
                    // если не удалось снять блокировку — продолжаем как заблокированный
                }
            }

            // Если отмечены и блокировка, и деактивация — показываем блокировку (как более понятную причину отказа).
            if (user.is_blocked == true)
            {
                if (user.blocked_until is DateTime until && until > DateTime.Now)
                {
                    ErrorMessage = $"Ваш аккаунт заблокирован до {until:yyyy-MM-dd HH:mm}." +
                                   (string.IsNullOrWhiteSpace(user.block_reason) ? "" : $" Причина: {user.block_reason}.");
                }
                else
                {
                    ErrorMessage = "Ваш аккаунт заблокирован администратором." +
                                   (string.IsNullOrWhiteSpace(user.block_reason) ? "" : $" Причина: {user.block_reason}.");
                }
                return Page();
            }

            if (user.is_deleted == true)
            {
                ErrorMessage = "Аккаунт деактивирован администратором.";
                return Page();
            }

            var claims = new List<Claim>
            {
                new(ClaimTypes.NameIdentifier, user.ID_user.ToString()),
                new(ClaimTypes.Name, user.full_name),
                new(ClaimTypes.Email, (user.email ?? string.Empty).Trim().Trim('"').ToLowerInvariant()),
                new(ClaimTypes.Role, user.role)
            };

            var identity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);
            var principal = new ClaimsPrincipal(identity);
            await HttpContext.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
            await HttpContext.SignInAsync(
                CookieAuthenticationDefaults.AuthenticationScheme,
                principal,
                new AuthenticationProperties { IsPersistent = true });

            if (!string.IsNullOrWhiteSpace(ReturnUrl) && Url.IsLocalUrl(ReturnUrl))
            {
                return Redirect(ReturnUrl);
            }

            return RedirectToPage("/Cabinet");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to authenticate user.");
            ErrorMessage = "Ошибка подключения к базе данных.";
            return Page();
        }
    }
}
