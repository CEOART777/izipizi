using System.ComponentModel.DataAnnotations;
using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;

namespace практика_2._0.Pages;

public class RegisterModel : PageModel
{
    private readonly AccessDbService _accessDbService;
    private readonly ILogger<RegisterModel> _logger;

    public RegisterModel(AccessDbService accessDbService, ILogger<RegisterModel> logger)
    {
        _accessDbService = accessDbService;
        _logger = logger;
    }

    [BindProperty]
    public RegisterInput Input { get; set; } = new();

    public string? ErrorMessage { get; private set; }

    public sealed class RegisterInput
    {
        [Required]
        public string FirstName { get; set; } = string.Empty;

        [Required]
        public string LastName { get; set; } = string.Empty;

        [Required]
        [EmailAddress]
        public string Email { get; set; } = string.Empty;

        [Required]
        public string Phone { get; set; } = string.Empty;

        [Required]
        [MinLength(4)]
        public string Password { get; set; } = string.Empty;

        [Required]
        public string ConfirmPassword { get; set; } = string.Empty;
    }

    public async Task<IActionResult> OnPostAsync(CancellationToken cancellationToken)
    {
        if (!ModelState.IsValid)
        {
            ErrorMessage = "Проверьте корректность заполнения формы.";
            return Page();
        }

        if (!string.Equals(Input.Password, Input.ConfirmPassword, StringComparison.Ordinal))
        {
            ErrorMessage = "Пароли не совпадают.";
            return Page();
        }

        try
        {
            var normalizedEmail = Input.Email.Trim().Replace("\"", string.Empty);
            var normalizedPhone = Input.Phone.Trim();
            var normalizedFirstName = Input.FirstName.Trim();
            var normalizedLastName = Input.LastName.Trim();
            var exists = await _accessDbService.EmailExistsAsync(normalizedEmail, cancellationToken);
            if (exists)
            {
                ErrorMessage = "Пользователь с таким email уже существует.";
                return Page();
            }

            var newUserId = await _accessDbService.CreateUserAsync(
                normalizedFirstName,
                normalizedLastName,
                normalizedEmail,
                normalizedPhone,
                Input.Password,
                cancellationToken);

            var createdUser = await _accessDbService.GetUserByIdAsync(newUserId, cancellationToken);
            if (createdUser is null)
            {
                ErrorMessage = "Регистрация не завершена: пользователь не найден в базе после сохранения.";
                return Page();
            }

            var claims = new List<Claim>
            {
                new(ClaimTypes.NameIdentifier, createdUser.ID_user.ToString()),
                new(ClaimTypes.Name, createdUser.full_name),
                new(ClaimTypes.Email, (createdUser.email ?? string.Empty).Trim().Trim('"').ToLowerInvariant()),
                new(ClaimTypes.Role, createdUser.role)
            };

            var identity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);
            var principal = new ClaimsPrincipal(identity);
            await HttpContext.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
            await HttpContext.SignInAsync(
                CookieAuthenticationDefaults.AuthenticationScheme,
                principal,
                new AuthenticationProperties { IsPersistent = true });

            return RedirectToPage("/UserProfile", new { userId = createdUser.ID_user });
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogWarning(ex, "Registration rejected.");
            ErrorMessage = ex.Message;
            return Page();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to register user.");
            ErrorMessage = "Ошибка при регистрации. Проверьте подключение и структуру таблицы USER.";
            return Page();
        }
    }
}
