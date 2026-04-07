using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using практика_2._0.Data;
using практика_2._0.Models;

namespace практика_2._0.Pages;

[Authorize]
public class PaymentModel : PageModel
{
    private readonly AccessDbService _accessDbService;
    private readonly ILogger<PaymentModel> _logger;

    public PaymentModel(AccessDbService accessDbService, ILogger<PaymentModel> logger)
    {
        _accessDbService = accessDbService;
        _logger = logger;
    }

    public CourseItem? Course { get; private set; }
    public UserItem? User { get; private set; }
    public EnrollmentItem? Enrollment { get; private set; }
    [BindProperty]
    public string Phone { get; set; } = string.Empty;
    [BindProperty]
    public int CoinsToUse { get; set; }
    public int AvailableCoins { get; private set; }
    public decimal FinalAmount { get; private set; }
    public string? ErrorMessage { get; private set; }
    public string? SuccessMessage { get; private set; }

    public async Task OnGetAsync(int? courseId, int? userId, CancellationToken cancellationToken)
    {
        if (HttpContext.User.IsInRole("teacher"))
        {
            Course = null;
            User = null;
            Enrollment = null;
            Phone = string.Empty;
            ErrorMessage = "Учитель не может оплачивать и записываться на курсы.";
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
            Enrollment = await _accessDbService.GetEnrollmentByUserAndCourseAsync(currentUserId.Value, currentCourseId, cancellationToken);
            var st = (Enrollment?.status ?? string.Empty).Trim();
            if (string.Equals(st, "cancelled", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(st, "refunded", StringComparison.OrdinalIgnoreCase))
            {
                Enrollment = null; // отменённые/возвращённые не должны скрывать форму оплаты
            }
            Phone = User?.phone ?? string.Empty;
            AvailableCoins = Math.Max(0, User?.balanse_coins ?? 0);
            if (Course is null)
            {
                FinalAmount = 0m;
            }
            else
            {
                var maxCoins = Math.Min(AvailableCoins, (int)Math.Floor(Math.Max(0m, Course.price)));
                var useCoins = Math.Max(0, Math.Min(CoinsToUse, maxCoins));
                FinalAmount = Math.Max(0m, Course.price - useCoins);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load payment data.");
            Course = null;
            User = null;
            Enrollment = null;
            Phone = string.Empty;
        }
    }

    public async Task<IActionResult> OnPostAsync(int courseId, CancellationToken cancellationToken)
    {
        try
        {
            if (HttpContext.User.IsInRole("teacher"))
            {
                return RedirectToPage("/Course", new { courseId });
            }

            var currentUserId = await ResolveCurrentUserIdAsync(null, cancellationToken);
            if (currentUserId is null)
            {
                return RedirectToPage("/Login");
            }

            var course = await _accessDbService.GetCourseByIdAsync(courseId, cancellationToken);
            if (course is null)
            {
                return RedirectToPage("/Index");
            }

            // Локальный режим: «оплата» всегда успешна после ввода телефона.
            if (string.IsNullOrWhiteSpace(Phone))
            {
                ErrorMessage = "Введите номер телефона для подтверждения.";
                await OnGetAsync(courseId, null, cancellationToken);
                return Page();
            }

            var user = await _accessDbService.GetUserByIdAsync(currentUserId.Value, cancellationToken);
            if (user is not null)
            {
                await _accessDbService.UpdateUserProfileAsync(
                    currentUserId.Value,
                    user.full_name,
                    user.email,
                    Phone.Trim(),
                    cancellationToken);
            }

            var availableCoins = Math.Max(0, user?.balanse_coins ?? 0);
            var maxUsable = (int)Math.Floor(Math.Max(0m, course.price));
            var normalizedCoinsToUse = Math.Max(0, Math.Min(CoinsToUse, Math.Min(availableCoins, maxUsable)));

            var method = "local";
            await _accessDbService.EnsureEnrollmentAndPaymentAsync(
                currentUserId.Value,
                courseId,
                method,
                course.price,
                normalizedCoinsToUse,
                cancellationToken);

            return RedirectToPage("/EnrollmentSuccess", new { courseId });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to complete payment and enrollment.");
            ErrorMessage = "Не удалось завершить оформление. Проверьте связи таблиц ENROLLMENTS, PAY, HOMEWORK.";
            await OnGetAsync(courseId, null, cancellationToken);
            return Page();
        }
    }

    private async Task<int?> ResolveCurrentUserIdAsync(int? routeUserId, CancellationToken cancellationToken)
    {
        // 1) Самый надёжный идентификатор — email (уникален). С него и начинаем.
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
            // Иногда в cookie/claims остаётся неверный ID (у конкретного пользователя),
            // из-за чего оплаты/записи уходят на "чужой" ID. Проверяем, что пользователь реально существует.
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
